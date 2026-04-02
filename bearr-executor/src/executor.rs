use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicBool},
    },
    task::{Context, Poll, Wake, Waker},
};

use flume::TrySendError;
use io_uring::{CompletionQueue, SubmissionQueue};

use crate::{DbRequest, DbResponse};

/// Simple waker that just uses an AtomicBool to track whether it's been woken or not
struct BoolWaker {
    woken: AtomicBool,
}
impl BoolWaker {
    /// Creates a new BoolWaker that starts in the woken state.
    /// We start in the woken state so newly spawned tasks are polled immediately
    /// and can start their I/O operations
    fn new() -> Arc<Self> {
        Arc::new(Self {
            woken: AtomicBool::new(true),
        })
    }
    fn is_woken(&self) -> bool {
        self.woken.load(atomic::Ordering::Acquire)
    }
    fn set_not_woken(&self) {
        self.woken.store(false, atomic::Ordering::Release);
    }
}

impl Wake for BoolWaker {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, atomic::Ordering::Release);
    }
}

/// Task representing an in-progress database operation
struct Task<'b> {
    future: Pin<Box<dyn Future<Output = DbResponse> + 'b>>,
    waker: Arc<BoolWaker>,
}

impl<'b> Task<'b> {
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = DbResponse> + 'b,
    {
        Self {
            future: Box::pin(future),
            waker: BoolWaker::new(),
        }
    }
}

/// Executor for handling database operations using io_uring for asynchronous I/O
pub struct Executor<'a, 'b> {
    /// io_uring submission queue
    s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
    /// io_uring completion queue
    c_queue: Arc<Mutex<CompletionQueue<'a>>>,
    /// Channel for receiving database operations to execute
    receiver: flume::Receiver<DbRequest>,
    /// Channel for sending back database operation results
    sender: flume::Sender<DbResponse>,
    /// Queue of responses that are ready to be sent back but haven't been sent yet
    to_send: VecDeque<DbResponse>,
    /// Tasks currently being executed
    tasks: Vec<Task<'b>>,
    /// Tasks waiting to be submitted to the s_queue once there's space
    submission_registry: Arc<Mutex<VecDeque<Waker>>>,
    /// Tasks waiting for a completion from the c_queue with a specific id
    completion_registry: Arc<Mutex<HashMap<u64, Waker>>>,
    /// Counter for generating unique ids for tasks
    id_counter: u64,
    /// Maximum number of tasks to execute concurrently
    max_tasks: usize,
}

impl<'a: 'b, 'b> Executor<'a, 'b> {
    pub fn new(
        s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
        c_queue: Arc<Mutex<CompletionQueue<'a>>>,
        receiver: flume::Receiver<DbRequest>,
        sender: flume::Sender<DbResponse>,
        max_tasks: usize,
    ) -> Self {
        Self {
            s_queue,
            c_queue,
            receiver,
            sender,
            to_send: VecDeque::new(),
            tasks: Vec::new(),
            submission_registry: Arc::new(Mutex::new(VecDeque::new())),
            completion_registry: Arc::new(Mutex::new(HashMap::new())),
            id_counter: 0,
            max_tasks,
        }
    }

    fn get_new_id(&mut self) -> u64 {
        let id = self.id_counter;
        self.id_counter += 1;
        id
    }

    fn spawn_read(
        &mut self,
        file: io_uring::types::Fixed,
        offset: u64,
        num_bytes: u32,
        buffer: Box<[u8]>,
    ) {
        let id = self.get_new_id();

        let s_queue = Arc::clone(&self.s_queue);
        let c_queue = Arc::clone(&self.c_queue);
        let submission_registry = Arc::clone(&self.submission_registry);
        let completion_registry = Arc::clone(&self.completion_registry);
        let read_fut = unsafe {
            crate::io::read(
                s_queue,
                c_queue,
                file,
                offset,
                num_bytes,
                buffer,
                id,
                submission_registry,
                completion_registry,
            )
        };

        self.tasks.push(Task::new(read_fut));
    }

    /// Spawns a new task for the given database operation
    fn spawn_operation(&mut self, operation: DbRequest) {
        match operation {
            DbRequest::Read {
                file,
                offset,
                num_bytes,
                buffer,
            } => {
                self.spawn_read(file, offset, num_bytes, buffer);
            }
        }
    }

    /// Main executor loop. Continuously polls active tasks, reacts to new requests and responses,
    /// and manages the submission and completion queues.
    pub fn run(&mut self) {
        loop {
            // Poll all active tasks
            let mut i = 0;
            while i < self.tasks.len() {
                let task = &mut self.tasks[i];
                if !task.waker.is_woken() {
                    i += 1;
                    continue;
                };

                // Mark the task as sleeping before polling so it can wake itself again when it's ready
                task.waker.set_not_woken();

                let waker = Waker::from(Arc::clone(&task.waker));
                let mut cx = Context::from_waker(&waker);
                match task.future.as_mut().poll(&mut cx) {
                    Poll::Ready(response) => {
                        // Task is done, remove it and add the response to the send queue
                        self.tasks.swap_remove(i);
                        self.to_send.push_back(response);
                    }
                    Poll::Pending => {
                        // task.waker.set_not_woken();
                        i += 1;
                    }
                }
            }

            // TODO: handle waking up kernel threads

            self.recv_requests_non_blocking(self.max_tasks.saturating_sub(self.tasks.len()));

            self.send_responses_non_blocking();

            self.react_submission_queue();

            self.react_completion_queue();

            if self.tasks.is_empty() {
                let recv_alive = self.react_no_tasks_blocking();
                if !recv_alive && self.tasks.is_empty() {
                    self.send_responses_blocking();
                    return;
                }
            }
        }
    }

    /// Tries to receive up to n requests without blocking and spawns tasks for them
    fn recv_requests_non_blocking(&mut self, n: usize) {
        for _ in 0..n {
            if let Ok(operation) = self.receiver.try_recv() {
                self.spawn_operation(operation);
            } else {
                break;
            }
        }
    }

    /// Tries to send as many responses as possible without blocking
    fn send_responses_non_blocking(&mut self) {
        while let Some(response) = self.to_send.pop_front() {
            match self.sender.try_send(response) {
                Ok(()) => {}
                Err(TrySendError::Full(response)) => {
                    self.to_send.push_front(response);
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    // If the receiver has been dropped, we can just drop the responses
                }
            }
        }
    }

    /// Sends all pending responses. Should only be used when shutting down the executor
    fn send_responses_blocking(&mut self) {
        while let Some(response) = self.to_send.pop_front() {
            let _ = self.sender.send(response);
        }
    }

    /// Syncs submission queue and wakes tasks waiting to submit
    fn react_submission_queue(&mut self) {
        let mut s_queue = self.s_queue.lock().unwrap();
        s_queue.sync();

        let mut s_registry = self.submission_registry.lock().unwrap();
        let submission_room = s_queue.capacity() - s_queue.len();

        for _ in 0..min(submission_room, s_registry.len()) {
            let waker = s_registry.pop_front().unwrap();
            waker.wake();
        }
    }

    /// Syncs completion queue and wakes tasks waiting for completions
    fn react_completion_queue(&mut self) {
        let mut c_queue = self.c_queue.lock().unwrap();
        c_queue.sync();

        let mut c_registry = self.completion_registry.lock().unwrap();
        if !c_registry.is_empty() {
            for entry in c_queue.by_ref() {
                let user_data = entry.user_data();
                if let Some(waker) = c_registry.remove(&user_data) {
                    waker.wake();
                }
            }
        }
    }

    /// Returns whether the receiving connection is still alive
    fn react_no_tasks_blocking(&mut self) -> bool {
        // No active tasks, block waiting for new operations
        if let Ok(operation) = self.receiver.recv() {
            self.spawn_operation(operation);
        } else {
            // All senders have been dropped, exit the executor loop
            return false;
        }

        // After blocking once, also add any more that we can without blocking
        while let Ok(operation) = self.receiver.try_recv() {
            self.spawn_operation(operation);
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Write},
        mem,
        os::fd::{AsRawFd, IntoRawFd},
    };

    use super::*;

    #[test]
    fn exec_test_add() {
        println!("{}", libc::EBADF);

        let (db_ops_sender, db_ops_receiver) = flume::bounded(100);
        let (db_responses_sender, db_responses_receiver) = flume::bounded(100);

        let poo_file = OpenOptions::new()
            .read(true)
            .open("/home/ari/BEARR/poo_file_uring.txt")
            .unwrap();
        let poo_fd = poo_file.into_raw_fd();
        // mem::forget(poo_file);

        std::thread::spawn(move || {
            let max_io_entries = 2048;
            let mut io_uring = io_uring::IoUring::builder()
                .setup_sqpoll(10000)
                .setup_r_disabled()
                .build(max_io_entries)
                .unwrap();

            io_uring.submitter().register_files(&[poo_fd]).unwrap();
            io_uring.submitter().register_enable_rings().unwrap();
            let (submitter, s_queue, c_queue) = io_uring.split();

            // submitter.register_files(&[poo_fd]).unwrap();
            // submitter.register_enable_rings().unwrap();

            let mut executor = Executor::new(
                Arc::new(Mutex::new(s_queue)),
                Arc::new(Mutex::new(c_queue)),
                db_ops_receiver,
                db_responses_sender,
                10,
            );
            executor.run();
        });

        let read_request = DbRequest::Read {
            file: io_uring::types::Fixed(poo_fd as u32),
            offset: 0,
            num_bytes: 10,
            buffer: vec![0; 1024].into_boxed_slice(),
        };

        db_ops_sender.send(read_request).unwrap();

        let res = db_responses_receiver.recv().unwrap();

        match res {
            DbResponse::ReadResult(result) => match result {
                Ok((buffer, num_bytes)) => {
                    println!("Read {} bytes: {:?}", num_bytes, &buffer[..num_bytes]);
                }
                Err((buffer, err)) => {
                    eprintln!("Read error: {}, buffer: {:?}", err, buffer);
                }
            },
        }
    }
}
