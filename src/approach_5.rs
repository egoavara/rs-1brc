use std::borrow::Cow;
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

pub fn run(path: &Path) {
    let file = std::fs::File::open(path).unwrap();
    let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };

    let mut offset = Some(0);
    let mut result: HashMap<Cow<str>, Data> = HashMap::with_capacity(100000);
    while let Some(o) = &offset {
        if *o >= mmap.len() {
            break;
        }
        let (key, val, next_offset) = next(&mmap, *o);
        offset = next_offset;
        let key = Cow::Borrowed(unsafe { std::str::from_utf8_unchecked(key) });
        let value = unsafe { std::str::from_utf8_unchecked(val) }.parse::<f64>().unwrap();
        result.entry(key)
            .and_modify(|e: &mut _| {
                e.min = e.min.min(value);
                e.max = e.max.max(value);
                e.sum += value;
                e.count += 1;
            })
            .or_insert_with(|| Data {
                min: value,
                max: value,
                sum: value,
                count: 1,
            });
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