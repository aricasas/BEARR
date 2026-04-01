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

struct BoolWaker {
    woken: AtomicBool,
}
impl BoolWaker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            woken: AtomicBool::new(true),
        })
    }
    fn is_woken(&self) -> bool {
        self.woken.load(atomic::Ordering::Acquire)
    }
    fn sleep(&self) {
        self.woken.store(false, atomic::Ordering::Release);
    }
}

impl Wake for BoolWaker {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, atomic::Ordering::Release);
    }
}

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

struct Executor<'a, 'b> {
    s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
    c_queue: Arc<Mutex<CompletionQueue<'a>>>,
    receiver: flume::Receiver<DbRequest>,
    sender: flume::Sender<DbResponse>,
    to_send: VecDeque<DbResponse>,
    tasks: Vec<Task<'b>>,
    submission_registry: Arc<Mutex<VecDeque<Waker>>>,
    completion_registry: Arc<Mutex<HashMap<u64, Waker>>>,
    id_counter: u64,
    max_tasks: usize,
}

impl<'a: 'b, 'b> Executor<'a, 'b> {
    fn new(
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
            to_send: VecDeque::with_capacity(max_tasks),
            tasks: Vec::with_capacity(max_tasks),
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

    fn run(&mut self) {
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
                task.waker.sleep();

                let waker = Waker::from(Arc::clone(&task.waker));
                let mut cx = Context::from_waker(&waker);
                match task.future.as_mut().poll(&mut cx) {
                    Poll::Ready(response) => {
                        // Task is done, remove it and add the response to the send queue
                        self.tasks.swap_remove(i);
                        self.to_send.push_back(response);
                    }
                    Poll::Pending => {
                        task.waker.sleep();
                        i += 1;
                    }
                }
            }

            self.recv_requests_non_blocking(self.max_tasks.saturating_sub(self.tasks.len()));

            self.send_responses_non_blocking();

            self.react_submission_queue();

            self.react_completion_queue();

            if self.tasks.is_empty() {
                let recv_alive = self.react_no_tasks_blocking();
                if !recv_alive && self.tasks.is_empty() && self.to_send.is_empty() {
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
