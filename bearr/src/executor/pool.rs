use std::{
    mem,
    os::fd::AsRawFd,
    sync::Arc,
    thread::{self, JoinHandle},
};

use bearr_error::DbError;
use flume::{Receiver, Sender};
use io_uring::{IoUring, cqueue, squeue};

use crate::{
    Database, DbResponse,
    executor::{DbOperation, DbRequest, DbRet, Executor},
};

pub struct WorkerPool {
    /// Worker threads
    workers: Vec<JoinHandle<Result<(), DbError>>>,
    /// Channel for sending database operations to execute
    submission: flume::Sender<DbRequest>,
    /// Channel for receiving back database operation results
    completion: flume::Receiver<DbResponse>,
    // database: Option<Database>, // TODO: Add support for opening multiple databases
    individual_senders: Vec<Sender<DbRequest>>, // Since these are individual, TODO: use somethign more efficient than flume
}

impl WorkerPool {
    pub fn new(num_threads: usize, max_task_per_thread: usize) -> Result<Self, DbError> {
        assert!(num_threads > 0);
        assert!(max_task_per_thread > 0);

        let (db_ops_sender, db_ops_receiver) = flume::bounded(2048);
        let (db_responses_sender, db_responses_receiver) = flume::bounded(2048);

        let main_ring: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder()
            .setup_single_issuer()
            .setup_sqpoll(100)
            .build(2048)?;

        let ring_fd = main_ring.as_raw_fd();

        let mut workers = Vec::with_capacity(num_threads);
        let mut individual_senders = Vec::with_capacity(num_threads);
        let (main_individual_sender, main_individual_receiver) = flume::bounded(2048);
        individual_senders.push(main_individual_sender);

        for _ in 0..num_threads - 1 {
            let ops_receiver = db_ops_receiver.clone();
            let res_sender = db_responses_sender.clone();
            let (individual_sender, individual_receiver) = flume::bounded(2048);
            individual_senders.push(individual_sender);

            let ring: IoUring = IoUring::builder()
                .setup_single_issuer()
                .setup_sqpoll(100)
                .setup_attach_wq(ring_fd) // TODO: Check if this is better than not doing it
                .build(2048)?;

            let handle = thread::spawn(move || {
                executor_main(
                    ring,
                    ops_receiver,
                    res_sender,
                    max_task_per_thread,
                    individual_receiver,
                )
            });
            workers.push(handle);
        }
        let handle = thread::spawn(move || {
            executor_main(
                main_ring,
                db_ops_receiver,
                db_responses_sender,
                max_task_per_thread,
                main_individual_receiver,
            )
        });
        workers.push(handle);

        Ok(Self {
            workers,
            submission: db_ops_sender,
            completion: db_responses_receiver,
            individual_senders,
        })
    }

    pub fn register_db(&self, database: Database) {
        let database = Arc::new(database);
        for (i, sender) in self.individual_senders.iter().enumerate() {
            sender
                .send(DbRequest {
                    request_id: i as u64,
                    request: DbOperation::RegisterDb {
                        database: database.clone(),
                    },
                })
                .unwrap();
        }

        for _ in 0..self.individual_senders.len() {
            let res = self.recv_response().expect("Executors should be alive");
            assert!(matches!(res.response, Ok(DbRet::None)));
            assert!((0..self.individual_senders.len() as u64).contains(&res.request_id));
        }
    }

    pub fn send_request(&self, request: DbRequest) {
        self.submission
            .send(request)
            .expect("Executors should be alive");
    }

    pub fn recv_response(&self) -> Option<DbResponse> {
        self.completion.recv().ok()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        let (x, mut y) = flume::bounded(1);
        mem::swap(&mut self.completion, &mut y);
        drop(x);
        drop(y);

        let (mut x, y) = flume::bounded(1);
        mem::swap(&mut self.submission, &mut x);
        drop(x);
        drop(y);

        for thread in self.workers.drain(..) {
            let _ = thread.join();
        }
    }
}

fn executor_main(
    ring: IoUring,
    ops_receiver: Receiver<DbRequest>,
    res_sender: Sender<DbResponse>,
    max_tasks: usize,
    individual_receiver: Receiver<DbRequest>,
) -> Result<(), DbError> {
    ring.submit()?; // TODO: Check if makes sense and necessary
    let mut executor = Executor::new(
        ring,
        ops_receiver,
        res_sender,
        max_tasks,
        individual_receiver,
    );
    executor.run();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        DbConfiguration, LsmConfiguration,
        executor::{DbOperation, DbRet},
    };

    use super::*;

    #[test]
    fn test_multi_ops() {
        let pool = WorkerPool::new(10, 100).unwrap();

        pool.send_request(DbRequest {
            request_id: 0,
            request: DbOperation::Create {
                name: PathBuf::from("poopy"),
                configuration: DbConfiguration {
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 4,
                        memtable_capacity: 100,
                        bloom_filter_bits: 5,
                    },
                    buffer_pool_capacity: 20,
                    write_buffering: 1,
                    readahead_buffering: 1,
                    wal_buffer_size: None,
                },
            },
        });
        let DbResponse {
            request_id,
            response: Ok(DbRet::DbHandle(database)),
        } = pool.recv_response().unwrap()
        else {
            panic!()
        };
        assert_eq!(request_id, 0);

        pool.register_db(database);

        pool.send_request(DbRequest {
            request_id: 1,
            request: DbOperation::Put { key: 10, value: 25 },
        });

        pool.send_request(DbRequest {
            request_id: 2,
            request: DbOperation::Put { key: 20, value: 35 },
        });
        pool.send_request(DbRequest {
            request_id: 3,
            request: DbOperation::Put { key: 30, value: 45 },
        });

        for _ in 0..3 {
            let res = pool.recv_response().unwrap();
            assert!(matches!(res.response, Ok(DbRet::None)));
        }

        pool.send_request(DbRequest {
            request_id: 4,
            request: DbOperation::Get { key: 10 },
        });
        pool.send_request(DbRequest {
            request_id: 5,
            request: DbOperation::Get { key: 20 },
        });
        pool.send_request(DbRequest {
            request_id: 6,
            request: DbOperation::Get { key: 30 },
        });
        pool.send_request(DbRequest {
            request_id: 7,
            request: DbOperation::Get { key: 40 },
        });

        for _ in 0..4 {
            let res = pool.recv_response().unwrap();
            if (4..=6u64).contains(&res.request_id) {
                assert!(
                    matches!(res.response, Ok(DbRet::Value(value)) if value == Some(10 * (res.request_id - 3) + 15))
                );
            } else {
                assert_eq!(res.request_id, 7);
                assert!(matches!(res.response, Ok(DbRet::Value(None))));
            };
        }

        pool.send_request(DbRequest {
            request_id: 8,
            request: DbOperation::Flush,
        });
        let res = pool.recv_response().unwrap();
        assert_eq!(res.request_id, 8);
        assert!(matches!(res.response, Ok(DbRet::None)));
    }
}
