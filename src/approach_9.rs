use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::simd::cmp::SimdPartialEq;
use std::simd::u8x32;

use hashbrown::HashMap;
use rayon::prelude::*;

#[derive(Debug)]
struct Data<'s> {
    key: &'s str,
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

const TABLE_SIZE: usize = 1 << 17;
const SEGMENT_SIZE: usize = 1 << 21;

pub fn run(path: &Path) {
    let file = std::fs::File::open(path).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };

    let cpu_count = std::thread::available_parallelism().unwrap();
    let parallel_count = usize::from(cpu_count) * 1;
    let chunk_size = mmap.len() / parallel_count;
    let chunks = (0..parallel_count)
        .into_iter()
        .map(|pos| optimize_position(&mmap, pos * chunk_size))
        .map_windows(|&[x, y]| (x, y))
        .collect::<Vec<_>>();
    let result = chunks
        .into_par_iter()
        .map(|(start, end)| {
            let mut offset = Some(start);
            let mut result = storage();
            while let Some(o) = &offset {
                if *o >= end {
                    break;
                }
                offset = step(&mmap, *o, &mut result);
            }
            result
        })
        .reduce(storage, |mut acc, mut x| {
            for i in 0..TABLE_SIZE {
                acc[i] = match (acc[i].take(), x[i].take()) {
                    (Some(a), Some(b)) => Some(Data {
                        key: a.key,
                        min: a.min.min(b.min),
                        max: a.max.max(b.max),
                        sum: a.sum + b.sum,
                        count: a.count + b.count,
                    }),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
            }
            acc
        });
}

#[inline]
fn storage<'a>() -> Vec<Option<Data<'a>>> {
    let mut result: Vec<Option<Data>> = Vec::with_capacity(TABLE_SIZE);
    for _ in 0..TABLE_SIZE {
        result.push(None);
    }
    result
}

#[inline]
fn optimize_position<'a>(mmap: &'a memmap2::Mmap, position: usize) -> usize {
    mmap[position..]
        .iter()
        .position(|&x| x == b'\n')
        .map(|x| position + x + 1)
        .unwrap_or_else(|| mmap.len())
}

#[inline]
fn fill_by_slice(data: &[u8]) -> u8x32 {
    if data.len() > 32 {
        return u8x32::from_slice(data);
    }
    let mut result = [0; 32];
    unsafe {
        core::ptr::copy_nonoverlapping(data.as_ptr(), result.as_mut_ptr(), data.len());
    }
    return u8x32::from_slice(&result);
}


fn step<'a: 'b, 'b>(mmap: &'a memmap2::Mmap, offset: usize, result: &'b mut Vec<Option<Data<'a>>>) -> Option<usize> {
    let (segment, next) = segmentation(&mmap, offset);
    let mut offset = 0;

    while let Some((key, value, next_offset)) = next_line(segment, offset) {
        do_line(key, value, result);
        offset = next_offset;
    }

    if next < mmap.len() {
        return Some(next);
    }
    None
}


#[inline]
fn segmentation<'a>(mmap: &'a memmap2::Mmap, offset: usize) -> (&'a [u8], usize) {
    let start = offset;
    if start + SEGMENT_SIZE < mmap.len() {
        for end in (start..(start + SEGMENT_SIZE).min(mmap.len())).rev() {
            if mmap[end] == b'\n' {
                return (&mmap[start..end], end + 1);
            }
        }
    }
    (&mmap[start..], mmap.len())
}

#[inline]
fn split3_segment(segment: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let mut segend_a = segment.len() / 3;
    let mut segend_b = segment.len() / 3 * 2;

    while segend_a > 0 && segment[segend_a] != b'\n' {
        segend_a -= 1;
    }
    while segend_b > 0 && segment[segend_b] != b'\n' {
        segend_b -= 1;
    }

    let a = &segment[..segend_a];
    let b = &segment[segend_a + 1..segend_b];
    let c = &segment[segend_b + 1..];

    (a, b, c)
}

#[inline]
fn next_line<'a>(segment: &'a [u8], start: usize) -> Option<(&'a str, f64, usize)> {
    if start >= segment.len() {
        return None;
    }

    let semicolon = find_pattern(segment, start, 0x3B);
    let linefeed = find_pattern(segment, start, 0x0A);
    let key = unsafe { std::str::from_utf8_unchecked(&segment[start..semicolon]) };
    let value = fast_float::parse::<f64, _>(&segment[semicolon + 1..linefeed]).unwrap();
    Some((key, value, linefeed + 1))
}

#[inline]
fn do_line<'a: 'b, 'b>(key: &'a str, val: f64, result: &'b mut Vec<Option<Data<'a>>>) -> Option<(&'a str, f64, usize)> {
    let mut hasher = ahash::AHasher::default();
    key.hash(&mut hasher);
    let idx = hasher.finish() as usize % TABLE_SIZE;
    let entry = &mut result[idx];
    if let Some(entry) = entry {
        entry.min = entry.min.min(val);
        entry.max = entry.max.max(val);
        entry.sum += val;
        entry.count += 1;
    } else {
        result[idx] = Some(Data {
            key: key,
            min: val,
            max: val,
            sum: val,
            count: 1,
        });
    }
    None
}

#[inline]
fn find_pattern(segment: &[u8], start: usize, pattern: u8) -> usize {
    if let Some(pos) = fill_by_slice(&segment[start..]).simd_eq(u8x32::splat(pattern)).first_set() {
        return start + pos;
    }
    for i in (start + 32)..segment.len() {
        if segment[i] == pattern {
            return i;
        }
    }
    segment.len()
}