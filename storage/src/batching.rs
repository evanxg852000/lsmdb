use crate::{Key, Value, TOMBSTONE};

#[derive(Debug, Default)]
pub struct BatchOperations {
    size_bytes: usize,
    operations: Vec<(Key, Value)>,
}

impl BatchOperations {
    pub fn new() -> Self {
        Self {
            size_bytes: 0,
            operations: vec![],
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.size_bytes += key.len() + value.len();
        self.operations.push((key, value));
    }

    pub fn delete(&mut self, key: Key) {
        let value = TOMBSTONE;
        self.size_bytes += key.len() + value.len();
        self.operations.push((key, value.to_vec()));
    }

    pub(crate) fn operations(&self) -> &[(Key, Value)] {
        &self.operations
    }

    pub(crate) fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}
