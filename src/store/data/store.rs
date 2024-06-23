use crate::{deque::Deque, store::data::Error, store::meta::Metadata};

pub struct Store {
    path: String,

    max_file_size: u64,

    deque: Deque,
}

impl Store {
    pub fn new(path: String, max_file_size: u64) -> Self {
        Self {
            path,
            max_file_size,
            deque: Deque::new(),
        }
    }

    pub fn delete(&self, meta: Metadata) -> Result<(), Error> {}
}
