#![feature(portable_simd)]

use std::hash::{BuildHasher, Hash};
use std::ops::{BitAnd, Not, Sub};
use std::path::Path;
use std::simd::cmp::SimdPartialEq;

mod approach_0;
mod approach_1;
mod approach_2;
mod approach_3;
// mod approach_4;

fn main() {
    // let path = Path::new("measurements_simple.txt");
    // let path = Path::new("measurements_1M.txt");
    let path = Path::new("measurements_100M.txt");
    // println!("approach_0: {:?}", timeit(|| approach_0::run(path), 32));
    // println!("approach_1: {:?}", timeit(|| approach_1::run(path), 2));
    // println!("approach_2: {:?}", timeit(|| approach_2::run(path), 2));
    println!("approach_3: {:?}", timeit(|| approach_3::run(path), 1));


    // let pattern = u8x32::splat(0x0A);
    // let cl_subtract = u8x32::splat(0x01);
    // let top_bitmask = u8x32::splat(0x80);
    // //
    // let target = test.as_bytes();
    // let target = u8x32::from_slice(&target[..32]);
    // //
    // let result = target.simd_eq(comparator);
    // println!("Result: {:032b}", result.to_bitmask());
    // result.first_set()
    //     .map(|x| println!("First set: {}", x))
    //     .unwrap_or_else(|| println!("No set bit found"));
    //
    // println!("{}", String::from_utf8_lossy(&test.as_bytes()[0..10]));
    // println!("{}", String::from_utf8_lossy(&test.as_bytes()[11..22]))
    //
    // let a = u8x4::from_array([1, 0, 0, 0]);
    // let b = u8x4::from_array([0, 0, 0, 1]);
    // let c = a.sub(b);
    // println!("{:?}", c.to_array());

    // let a = 0x01000000_u32;
    // let clsub = 0x01010101_u32;
    // let ofprot = 0x80808080_u32;
    //
    // println!("{:032b}", a);
    // println!("----------------------------");
    // println!("{:032b}", a.wrapping_sub(clsub));
    // println!("----------------------------");
    // println!("{:032b}", (a.not()).bitand(ofprot));
    // println!("----------------------------");
    // println!("{:032b}", (a.wrapping_sub(clsub)).bitand(a.not().bitand(ofprot)));
    // println!("============================");
    // let a = b";;;;".to_owned();
    // let a = u32::from_be_bytes(a);
    // let b = b":;;;".to_owned();
    // let b = u32::from_be_bytes(b);
    // let c = a ^ b;
    //
    //
    // println!("{:032b}", a);
    // println!("{:032b}", c);
    // println!("----------------------------");
    // println!("{:032b}", c.wrapping_sub(clsub));
    // println!("----------------------------");
    // println!("{:032b}", (!c & ofprot));
    // println!("----------------------------");
    // println!("{:032b}", (c.wrapping_sub(clsub)) & (!c & ofprot));
}

fn timeit<F: Fn() -> ()>(f: F, count: usize) -> std::time::Duration {
    let start = std::time::Instant::now();
    for _ in 0..count {
        f();
    }
    start.elapsed() / count as u32
}