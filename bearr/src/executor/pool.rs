use std::{
    mem,
    os::fd::AsRawFd,
    thread::{self, JoinHandle},
};

use bearr_error::DbError;
use flume::{Receiver, Sender};
use io_uring::{IoUring, cqueue, squeue};

use crate::{
    DbResponse,
    executor::{DbRequest, Executor},
};

pub struct WorkerPool {
    /// Worker threads
    workers: Vec<JoinHandle<Result<(), DbError>>>,
    /// Channel for sending database operations to execute
    submission: flume::Sender<DbRequest>,
    /// Channel for receiving back database operation results
    completion: flume::Receiver<DbResponse>,
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

        for _ in 0..num_threads - 1 {
            let ops_receiver = db_ops_receiver.clone();
            let res_sender = db_responses_sender.clone();

            let ring: IoUring = IoUring::builder()
                .setup_single_issuer()
                .setup_sqpoll(100)
                .setup_attach_wq(ring_fd) // TODO: Check if this is better than not doing it
                .build(2048)?;

            let handle = thread::spawn(move || {
                executor_main(ring, ops_receiver, res_sender, max_task_per_thread)
            });
            workers.push(handle);
        }
        let handle = thread::spawn(move || {
            executor_main(
                main_ring,
                db_ops_receiver,
                db_responses_sender,
                max_task_per_thread,
            )
        });
        workers.push(handle);

        Ok(Self {
            workers,
            submission: db_ops_sender,
            completion: db_responses_receiver,
        })
    }

    pub fn send_request(&self, request: DbRequest) {
        self.submission.send(request).unwrap();
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
) -> Result<(), DbError> {
    ring.submit()?; // TODO: Check if makes sense and necessary
    let mut executor = Executor::new(ring, ops_receiver, res_sender, max_tasks);
    executor.run();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        pin::Pin,
    };

    use io_uring::types::Fd;

    use io::{open, write};

    use crate::{
        DbConfiguration, LsmConfiguration,
        executor::{DbOperation, DbRet, io},
    };

    use super::*;

    #[test]
    fn test_multi_ops() {
        let pool = WorkerPool::new(10, 100).unwrap();

        pool.send_request(DbRequest {
            user_data: 0,
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

        let res = pool.recv_response().unwrap();
        assert_eq!(res.user_data, 0);

        let DbRet::DbHandle(database) = res.response.unwrap() else {
            panic!()
        };

        // database.get(8);
    }
}
