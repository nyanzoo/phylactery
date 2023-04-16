use tonic::Status;

use crate::communication::{self, submission, File};

#[derive(Clone, Debug)]
pub struct Close {
    pub name: String,
}

impl TryFrom<communication::Close> for Close {
    type Error = Status;

    fn try_from(value: communication::Close) -> Result<Self, Self::Error> {
        let communication::Close { name } = value;

        Ok(Close { name })
    }
}

#[derive(Clone, Debug)]
pub struct Copy {
    pub src: Vec<File>,
    pub dst: File,
}

impl TryFrom<communication::Copy> for Copy {
    type Error = Status;

    fn try_from(value: communication::Copy) -> Result<Self, Self::Error> {
        let communication::Copy { src, dst } = value;
        if src.is_empty() {
            return Err(Status::invalid_argument("src must be at least one file"));
        }
        if dst.is_none() {
            return Err(Status::invalid_argument("dst must be set"));
        }

        Ok(Copy {
            src,
            dst: dst.unwrap(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Delete {
    pub name: String,
}

impl TryFrom<communication::Delete> for Delete {
    type Error = Status;

    fn try_from(value: communication::Delete) -> Result<Self, Self::Error> {
        let communication::Delete { name } = value;

        Ok(Delete { name })
    }
}

#[derive(Clone, Debug)]
pub struct Open {
    pub file: File,
}

impl TryFrom<communication::Open> for Open {
    type Error = Status;

    fn try_from(value: communication::Open) -> Result<Self, Self::Error> {
        let communication::Open { file } = value;
        if file.is_none() {
            return Err(Status::invalid_argument("file must be set"));
        }

        Ok(Open {
            file: file.unwrap(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Read {
    pub src: String,
    pub dst: File,
}

impl TryFrom<communication::Read> for Read {
    type Error = Status;

    fn try_from(value: communication::Read) -> Result<Self, Self::Error> {
        let communication::Read { src, dst } = value;
        if dst.is_none() {
            return Err(Status::invalid_argument("dst must be set"));
        }

        Ok(Read {
            src,
            dst: dst.unwrap(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Write {
    pub src: File,
    pub dst: String,
}

impl TryFrom<communication::Write> for Write {
    type Error = Status;

    fn try_from(value: communication::Write) -> Result<Self, Self::Error> {
        let communication::Write { src, dst } = value;
        if src.is_none() {
            return Err(Status::invalid_argument("src must be set"));
        }

        Ok(Write {
            src: src.unwrap(),
            dst,
        })
    }
}

#[derive(Clone, Debug)]
pub enum Operation {
    Copy(Copy),
    Close(Close),
    Delete(Delete),
    Open(Open),
    Read(Read),
    Write(Write),
}

impl TryFrom<submission::Operation> for Operation {
    type Error = Status;

    fn try_from(value: submission::Operation) -> Result<Self, Self::Error> {
        match value {
            submission::Operation::Close(close) => Ok(Operation::Close(close.try_into()?)),
            submission::Operation::Copy(copy) => Ok(Operation::Copy(copy.try_into()?)),
            submission::Operation::Delete(delete) => Ok(Operation::Delete(delete.try_into()?)),
            submission::Operation::Open(open) => Ok(Operation::Open(open.try_into()?)),
            submission::Operation::Read(read) => Ok(Operation::Read(read.try_into()?)),
            submission::Operation::Write(write) => Ok(Operation::Write(write.try_into()?)),
        }
    }
}
