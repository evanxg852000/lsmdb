mod dma_file;

use std::{fs::OpenOptions, os::unix::prelude::OpenOptionsExt, time::Instant, io::Write};

use crate::dma_file::DmaFile;

fn main() {
    println!("Hello, world!");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("foo1.db").unwrap();
    let now = Instant::now();
    file.write_all(b"key1,value1").unwrap();
    //file.flush()
    println!("time: {}", now.elapsed().as_micros());

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open("foo.db").unwrap();

    let now = Instant::now();
    let mut dma_file = DmaFile::new(file, 512);
    let v = b"key1,value1";
    dma_file.write(v.as_slice()).unwrap();
    println!("time: {}", now.elapsed().as_micros());

    let v = b"key2,value2";
    dma_file.write(v).unwrap();
}
