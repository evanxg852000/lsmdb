use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{error::LiteDbResult, KVIterator, Key, Value};

#[derive(PartialEq)]
struct ItemPack(usize, Key, Value);

impl Eq for ItemPack {}

impl PartialOrd for ItemPack {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let first = other.1.partial_cmp(&self.1);
        match first {
            Some(ordering) if ordering == Ordering::Equal => {
                // if same key, we pick the one from the iterator with the largest index
                // because it is the most up to date(newest) iter
                self.0.partial_cmp(&other.0)
            }
            None => {
                // if same key, we pick the one from the iterator with the largest index
                // because it is the most up to date(newest) iter
                self.0.partial_cmp(&other.0)
            }
            _ => first,
        }
    }
}

impl Ord for ItemPack {
    fn cmp(&self, other: &ItemPack) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// An iterator that implements merge sort while taking into account
/// iterators are ordered from least recent changes to more recent.
///
/// When there is duplicate from multiple iterators, the upper iterator
/// is picked.
/// Order is important (from oldest to newest)
pub(crate) struct CombineIterator {
    iterators: Vec<KVIterator>,
    sorter: BinaryHeap<ItemPack>,
}

impl CombineIterator {
    pub(crate) fn try_new(mut iterators: Vec<KVIterator>) -> LiteDbResult<Self> {
        let mut sorter = BinaryHeap::new();
        for (idx, it) in iterators.iter_mut().enumerate() {
            if let Some(result) = it.next() {
                let (k, v) = result?;
                sorter.push(ItemPack(idx, k, v))
            }
        }
        Ok(Self { iterators, sorter })
    }

    fn advance(&mut self, iterator_idx: usize) -> LiteDbResult<()> {
        if let Some(result) = self.iterators[iterator_idx].next() {
            let (key, value) = result?;
            self.sorter.push(ItemPack(iterator_idx, key, value))
        }
        Ok(())
    }
}

impl Iterator for CombineIterator {
    type Item = LiteDbResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.sorter.pop() {
            Some(entry) => {
                if let Err(err) = self.advance(entry.0) {
                    return Some(Err(err));
                }
                entry
            }
            None => return None,
        };

        // keep skipping while hitting the same key
        while let Some(ItemPack(_, key, _)) = self.sorter.peek() {
            if item.1 != *key {
                break;
            }

            let entry = self.sorter.pop().unwrap();
            if let Err(err) = self.advance(entry.0) {
                return Some(Err(err));
            }
        }

        Some(Ok((item.1, item.2)))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use crate::{error::LiteDbResult, mem_table::MemTable, Scannable};

    use super::CombineIterator;

    fn create_mem_table(path: &Path, id: u64, data: Vec<(&str, &str)>) -> Arc<MemTable> {
        let mem_table = MemTable::open(path.to_path_buf(), id).unwrap();
        for (k, v) in data {
            mem_table.set(k.as_bytes(), v.as_bytes()).unwrap();
        }
        Arc::new(mem_table)
    }

    #[test]
    fn test_combine_iterator() -> LiteDbResult<()> {
        let temp_dir = tempfile::tempdir()?;

        let mem1 = create_mem_table(temp_dir.path(), 1, vec![("a", "a"), ("b", "b")]);
        let mem2 = create_mem_table(temp_dir.path(), 2, vec![("c", "c"), ("b", "b1")]);
        let mem3 = create_mem_table(temp_dir.path(), 3, vec![("a", "a1"), ("d", "d")]);
        let iterators = vec![
            mem1.scan(&None, &None),
            mem2.scan(&None, &None),
            mem3.scan(&None, &None),
        ];
        let combine_iter = CombineIterator::try_new(iterators).unwrap();

        let expected = vec![
            ("a".to_string(), "a1".to_string()),
            ("b".to_string(), "b1".to_string()),
            ("c".to_string(), "c".to_string()),
            ("d".to_string(), "d".to_string()),
        ];
        let actual = combine_iter
            .map(|result| {
                let (k, v) = result.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect::<Vec<_>>();

        assert_eq!(expected, actual);
        Ok(())
    }
}
