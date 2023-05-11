use std::os::unix::prelude::FileExt;

pub enum Operation<'a> {
    Read {
        data: &'a [u8],
        file: &'a str, // full path to file
        offset: u64,
        ret: crossbeam_channel::Sender<std::io::Result<()>>,
    },
    Write {
        data: &'a [u8],
        file: &'a str, // full path to file
        offset: u64,
        ret: crossbeam_channel::Sender<std::io::Result<()>>,
    },
}

pub struct OperationReader<'a>(std::sync::mpsc::Receiver<Operation<'a>>);

struct IORunner<'a> {
    op_reader: OperationReader<'a>,
    pool: rayon::ThreadPool,
}

impl<'a> IORunner<'a> {
    fn new(op_reader: OperationReader<'a>) -> Self {
        Self {
            op_reader,
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .build()
                .unwrap(),
        }
    }

    fn run(&mut self) {
        loop {
            match self.op_reader.0.recv() {
                Ok(Operation::Read {
                    data,
                    file,
                    offset,
                    ret,
                }) => {
                    let data = data.to_vec();
                    self.pool.spawn(move || {
                        let ret = std::fs::OpenOptions::new()
                            .read(true)
                            .open(file)
                            .and_then(|mut f| f.read_exact_at(&mut data[..], offset))
                            .map(|_| ());
                        ret.send(ret).expect("failed to send result");
                    });
                }
                Ok(Operation::Write {
                    data,
                    file,
                    offset,
                    ret,
                }) => {
                    let data = data.to_vec();
                    self.pool.spawn(move || {
                        let ret = std::fs::OpenOptions::new()
                            .write(true)
                            .open(file)
                            .and_then(|mut f| f.write_all_at(&data[..], offset))
                            .map(|_| ());
                        ret.send(ret).expect("failed to send result");
                    });
                }
                Err(_) => break,
            }
        }
    }
}
