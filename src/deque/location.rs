use std::{path::PathBuf, rc::Rc};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Location {
    pub dir: Rc<PathBuf>,
    pub file: u64,
}

impl Location {
    pub fn new(dir: impl Into<PathBuf>, file: u64) -> Self {
        Self {
            dir: Rc::new(dir.into()),
            file,
        }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(self.dir.as_ref()).join(format!("{:08x}.bin", self.file))
    }

    pub fn tombstone_path(&self) -> PathBuf {
        PathBuf::from(self.dir.as_ref()).join(format!("{:08x}.tombstone", self.file))
    }

    pub fn move_forward(&mut self) {
        *self = Self {
            dir: self.dir.clone(),
            file: self.file + 1,
        };
    }
}
