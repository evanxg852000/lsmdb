use core::{
    ptr::NonNull,
    slice,
};
use std::alloc;
use std::{fs::File, io::{Write, Seek, self, SeekFrom}};

struct AlignedBuffer {
    ptr: NonNull<u8>,
    buffer_size: usize,
    rest_pos: usize,
    align: usize,
}

impl Drop for AlignedBuffer {
    #[inline]
    fn drop(&mut self) {
        if self.buffer_size != 0 {
            unsafe {
                alloc::dealloc(self.ptr.as_ptr(), alloc::Layout::from_size_align_unchecked(
                    self.buffer_size,
                    self.align,
                ));
            }
        }
    }
}

impl AlignedBuffer {
    #[inline]
    pub fn new(data: &[u8], align: usize) -> Self {
        let (buffer_size, rest_pos) = if data.len() < align {
            (align, 0)
        } else {
            let buffer_size = data.len().next_power_of_two();
            let rest = data.len() % align;
            (buffer_size, data.len() - rest)
        };
        let raw_ptr = unsafe {
            alloc::alloc(alloc::Layout::from_size_align_unchecked(
                buffer_size,
                align,
            ))
        };
        unsafe {
            core::ptr::copy_nonoverlapping(
                data.as_ptr(),
                raw_ptr,
                data.len(),
            );
        }
        Self {
            ptr: NonNull::new(raw_ptr).unwrap(),
            buffer_size,
            rest_pos,
            align,
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.buffer_size) }
    }

    pub fn rest(&self) -> usize {
        self.rest_pos
    }
}

pub struct DmaFile{
    file: File,
    rest: Vec<u8>,
    align: usize,
}

impl DmaFile {
    pub fn new(file: File, align: usize) -> Self {
        Self {
            file,
            rest: vec![],
            align,
        }
    }

    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Position the write handle.
        if !self.rest.is_empty() {
            let seek_value = self.align as i64;
            self.file.seek(SeekFrom::Current(-seek_value))?;
        }
       
        // Combine new incoming data with residual data.
        self.rest.extend_from_slice(buf);
        let aligned_buffer = AlignedBuffer::new(self.rest.as_slice(), self.align);
        self.rest.drain(0..aligned_buffer.rest());
        self.file.write(aligned_buffer.as_slice())
    }
}
