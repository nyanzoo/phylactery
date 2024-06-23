use std::{path::PathBuf, rc::Rc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Location {
    pub dir: Rc<String>,
    pub file: u64,
}

impl Location {
    pub fn new(dir: impl ToString, file: u64) -> Self {
        Self {
            dir: Rc::new(dir.to_string()),
            file,
        }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(self.dir.as_ref()).join(format!("{:016x}.bin", self.file))
    }

    pub fn tombstone_path(&self) -> PathBuf {
        PathBuf::from(self.dir.as_ref()).join(format!("{:016x}.tombstone", self.file))
    }

    pub fn move_forward(&mut self) {
        *self = Self::new(self.dir.clone(), self.file + 1);
    }
}
