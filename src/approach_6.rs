use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::simd::cmp::SimdPartialEq;
use std::simd::u8x32;

use hashbrown::HashMap;
use rayon::prelude::*;

#[derive(Debug)]
struct Data {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

const TABLE_SIZE: usize = 1 << 17;

pub fn run(path: &Path) {
    let file = std::fs::File::open(path).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };

    let mut offset = Some(0);
    let mut result: Vec<Option<Data>> = Vec::with_capacity(TABLE_SIZE);
    for _ in 0..TABLE_SIZE {
        result.push(None);
    }

    while let Some(o) = &offset {
        if *o >= mmap.len() {
            break;
        }
        let (key, val, next_offset) = next(&mmap, *o);
        offset = next_offset;
        let key = Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(key) });
        let value = unsafe { std::str::from_utf8_unchecked(val) }.parse::<f64>().unwrap();
        let mut hasher = ahash::AHasher::default();
        key.hash(&mut hasher);
        let idx = hasher.finish() as usize % TABLE_SIZE;
        let entry = &mut result[idx];
        if let Some(entry) = entry {
            entry.min = entry.min.min(value);
            entry.max = entry.max.max(value);
            entry.sum += value;
            entry.count += 1;
        } else {
            result[idx] = Some(Data {
                min: value,
                max: value,
                sum: value,
                count: 1,
            });
        }
    }
    // println!("{:#?}", result);
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

fn next<'a>(mmap: &'a memmap2::Mmap, offset: usize) -> (&'a [u8], &'a [u8], Option<usize>) {
    let mut start = offset;
    let linefeed = fill_by_slice(&mmap[start..]).simd_eq(u8x32::splat(0x0A)).first_set();
    let semicolon = fill_by_slice(&mmap[start..]).simd_eq(u8x32::splat(0x3B)).first_set();
    match (linefeed, semicolon) {
        (Some(l), Some(s)) => {
            (&mmap[start..start + s], &mmap[start + s + 1..start + l], Some(start + l + 1))
        }
        (Some(l), None) => {
            unreachable!()
        }
        (None, Some(s)) => {
            for i in 32..mmap.len() {
                if mmap[start + i] == 0x0A {
                    return (&mmap[start..start + s], &mmap[start + s + 1..start + i], Some(start + i + 1));
                }
            }
            (&mmap[start..], &mmap[start + s + 1..], None)
        }
        (None, None) => {
            (&mmap[start..], &mmap[start..], None)
        }
    }
}