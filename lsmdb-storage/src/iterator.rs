use std::collections::BinaryHeap;

use crate::{error::LiteDbResult, KVIterator, Key, Value};

/// An iterator that implements merge sort while taking into account
/// iterators are ordered from more recent changes to least recent.
///
/// When there is duplicate from multiple iterators, the front iterator
pub(crate) struct CombineIterator {
    iterators: Vec<KVIterator>, // order is important (from newest to oldest)
    sorter: BinaryHeap<(usize, Key, Value)>,
}

impl CombineIterator {
    pub(crate) fn try_new(mut iterators: Vec<KVIterator>) -> LiteDbResult<Self> {
        let mut sorter = BinaryHeap::new();
        for (idx, it) in iterators.iter_mut().enumerate() {
            if let Some(result) = it.next() {
                let (k, v) = result?;
                sorter.push((idx, k, v))
            }
        }

        Ok(Self { iterators, sorter })
    }

    fn advance(&mut self, iterator_idx: usize) -> LiteDbResult<()> {
        if let Some(result) = self.iterators[iterator_idx].next() {
            let (key, value) = result?;
            self.sorter.push((iterator_idx, key, value))
        }
        Ok(())
    }
}

impl Iterator for CombineIterator {
    type Item = LiteDbResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = match self.sorter.pop() {
            Some(entry) => {
                if let Err(err) = self.advance(entry.0) {
                    return Some(Err(err));
                }
                entry
            }
            None => return None,
        };

        while let Some((_, key, _)) = self.sorter.peek() {
            if item.1 != *key {
                break;
            }

            let entry = self.sorter.pop().unwrap();
            if let Err(err) = self.advance(item.0) {
                return Some(Err(err));
            }
            if item.0 > entry.0 {
                // is entry is from a more recent sstable
                item = entry
            }
        }

        Some(Ok((item.1, item.2)))
    }
}
