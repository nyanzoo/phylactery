use std::{collections::BTreeMap, hash::Hash, sync::mpsc, thread};

use log::{error, trace};

use crate::{calculate_hash, Error};

pub trait Service {
    type Request: Hash + Send;
    type Response;

    fn process(&self, request: Self::Request) -> Result<Self::Response, Error>;

    fn forward(&self, request: Self::Response) -> Result<(), Error>;
}

pub trait ServiceBuilder {
    type Service: Service;

    fn build(&self) -> Self::Service;
}

/// The scheduler will split the work to the store
pub struct Scheduler<S>
where
    S: Service,
{
    threads: Vec<thread::JoinHandle<()>>,
    senders: Vec<mpsc::Sender<S::Request>>,
    /// this is the lookup of hash to thread id.
    /// the hash is calculated from the key or the queue name.
    lookup: BTreeMap<u64, usize>,

    thread_round_robin: usize,
}

impl<S> Scheduler<S>
where
    S: Service + Send + 'static,
{
    /// Create a new scheduler
    pub fn new(count: u8, service_builder: impl ServiceBuilder<Service = S>) -> Self {
        trace!("creating scheduler with {count} threads");
        let mut threads = vec![];
        let mut senders = vec![];
        for i in 0..count {
            let service = service_builder.build();
            let (tx, rx) = mpsc::channel();
            let thread = thread::spawn(move || loop {
                let request = rx.recv().expect("failed to receive request");

                match service.process(request) {
                    Err(err) => {
                        error!("failed to process request: {err}");
                    }
                    Ok(response) => {
                        if let Err(err) = service.forward(response) {
                            error!("failed to send response: {err}");
                        }
                    }
                }
            });
            threads.push(thread);
            senders.push(tx);
        }

        Self {
            threads,
            senders,
            lookup: BTreeMap::new(),
            thread_round_robin: 0,
        }
    }

    /// Schedule a request
    pub fn schedule(&mut self, request: S::Request) {
        let hash = calculate_hash(&request);
        let thread_id = match self.lookup.get(&hash).copied() {
            Some(thread_id) => thread_id,
            None => {
                let thread_id = self.thread_round_robin;
                self.thread_round_robin = (self.thread_round_robin + 1) % self.threads.len();

                self.lookup.insert(hash, thread_id);

                thread_id
            }
        };
        self.senders[thread_id]
            .send(request)
            .expect("failed to send request");
    }

    /// Add a hash to the lookup
    pub fn add_hash(&mut self, hash: u64, thread_id: u8) {
        self.lookup.insert(hash, usize::from(thread_id));
    }
}
