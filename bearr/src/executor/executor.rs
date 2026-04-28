use std::{
    cell::RefCell,
    cmp::min,
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{self, AtomicBool},
    },
    task::{Context, Poll, Wake, Waker},
};

use flume::TrySendError;

use crate::{DbRequest, DbResponse, executor::io::IoId};

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

type TaskId = u64;

/// Task representing an in-progress database operation
struct Task {
    future: DbOpFuture,
    waker: Arc<BoolWaker>,
    task_id: TaskId,
}

impl Task {
    fn new(future: DbOpFuture, task_id: TaskId) -> Self {
        Self {
            future,
            waker: BoolWaker::new(),
            task_id,
        }
    }
}

thread_local! {
    static CURRENT_TASK_CONTEXT: RefCell<Option<Rc<RefCell<CurrentTaskContext>>>> = const { RefCell::new(None) };
}
pub struct CurrentTaskContext {
    /// io_uring instance
    ring: io_uring::IoUring,
    /// Wakers for tasks waiting to be submitted to the s_queue once there's space
    submission_registry: VecDeque<Waker>,
    /// Stores wakers for tasks waiting for the completion of an IO id
    completion_registry: HashMap<IoId, Waker>,
    /// Map from IO ids to their completion codes, set when a completion is received from the c_queue
    completion_codes: HashMap<IoId, i32>,
    /// The current task's id
    task_id: TaskId,
}

impl CurrentTaskContext {
    pub fn get() -> Rc<RefCell<CurrentTaskContext>> {
        CURRENT_TASK_CONTEXT
            .with(|x| x.borrow_mut().clone())
            .expect("Calling get_context outside of a task context")
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn register_submission_wait(&mut self, waker: Waker) {
        self.submission_registry.push_back(waker);
    }

    pub fn register_completion_wait(&mut self, io_id: IoId, waker: Waker) {
        self.completion_registry.insert(io_id, waker);
    }

    pub fn consume_completion(&mut self, io_id: IoId) -> Option<i32> {
        self.completion_codes.remove(&io_id)
    }

    pub fn ring(&mut self) -> &mut io_uring::IoUring {
        &mut self.ring
    }
}

pub type DbOpFuture = Pin<Box<dyn Future<Output = DbResponse> + Send>>;
/// Executor for handling database operations using io_uring for asynchronous I/O
pub struct Executor {
    context: Rc<RefCell<CurrentTaskContext>>,

    /// Channel for receiving database operations to execute
    receiver: flume::Receiver<DbRequest>,
    /// Channel for sending back database operation results
    sender: flume::Sender<DbResponse>,
    /// Queue of responses that are ready to be sent back but haven't been sent yet
    to_send: VecDeque<DbResponse>,
    /// Tasks currently being executed
    tasks: Vec<Task>,
    /// Counter for generating unique ids for tasks
    task_id_counter: TaskId,
    /// Maximum number of tasks to execute concurrently
    max_tasks: usize,
}

impl Executor {
    pub fn new(
        ring: io_uring::IoUring,
        receiver: flume::Receiver<DbRequest>,
        sender: flume::Sender<DbResponse>,
        max_tasks: usize,
    ) -> Self {
        Self {
            context: Rc::new(RefCell::new(CurrentTaskContext {
                ring,
                submission_registry: VecDeque::new(),
                completion_registry: HashMap::new(),
                completion_codes: HashMap::new(),
                task_id: 0,
            })),
            receiver,
            sender,
            to_send: VecDeque::new(),
            tasks: Vec::new(),
            task_id_counter: 0,
            max_tasks,
        }
    }

    fn get_new_task_id(&mut self) -> TaskId {
        let task_id = self.task_id_counter;
        self.task_id_counter += 1;
        task_id
    }

    /// Spawns a new task for the given database operation
    fn spawn_operation(&mut self, operation: DbRequest) {
        // let task_id = self.get_new_task_id();
        // let task = Task::new(operation, task_id);
        // self.tasks.push(task);

        // match operation {
        //     DbRequest::Read {
        //         file,
        //         offset,
        //         num_bytes,
        //         buffer,
        //     } => {
        //         let op = unsafe { Box::pin(crate::io::read(file, offset, num_bytes, buffer)) };
        //         self.tasks.push(Task::new(op, task_id));
        //     }
        //     DbRequest::Write {
        //         file,
        //         offset,
        //         num_bytes,
        //         buffer,
        //     } => {
        //         let op = unsafe { Box::pin(crate::io::write(file, offset, num_bytes, buffer)) };
        //         self.tasks.push(Task::new(op, task_id));
        //     }
        // };
    }

    /// Main executor loop. Continuously polls active tasks, reacts to new requests and responses,
    /// and manages the submission and completion queues.
    pub fn run(&mut self) {
        let old_ctx = CURRENT_TASK_CONTEXT.replace(Some(self.context.clone()));
        if old_ctx.is_some() {
            panic!("Can only run one executor at a time per thread");
        }

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

                {
                    self.context.borrow_mut().task_id = task.task_id;
                }

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
                    break;
                }
            }
        }

        CURRENT_TASK_CONTEXT.replace(None);
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
        let context = &mut *self.context.borrow_mut();

        let mut s_queue = context.ring.submission();
        s_queue.sync();

        let submission_room = s_queue.capacity() - s_queue.len();

        let s_registry = &mut context.submission_registry;

        for _ in 0..min(submission_room, s_registry.len()) {
            let waker = s_registry.pop_front().unwrap();
            waker.wake();
        }
    }

    /// Syncs completion queue and wakes tasks waiting for completions
    fn react_completion_queue(&mut self) {
        let context = &mut *self.context.borrow_mut();

        context.ring.completion().sync();

        let c_ring = &mut context.ring.completion();
        let c_registry = &mut context.completion_registry;
        let completion_codes = &mut context.completion_codes;
        if !c_registry.is_empty() {
            for entry in c_ring.by_ref() {
                let user_data = entry.user_data();
                if let Some(waker) = c_registry.remove(&user_data) {
                    completion_codes.insert(user_data, entry.result());

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
    // use std::{fs::OpenOptions, os::fd::AsRawFd};

    use super::*;

    #[test]
    fn exec_test_add() {
        let (db_ops_sender, db_ops_receiver) = flume::bounded(100);
        let (db_responses_sender, db_responses_receiver) = flume::bounded(100);

        std::thread::spawn(move || {
            let max_io_entries = 2048;

            let ring = io_uring::IoUring::builder()
                .setup_single_issuer()
                .setup_sqpoll(10000)
                .build(max_io_entries)
                .unwrap();

            ring.submit().unwrap();

            let mut executor = Executor::new(ring, db_ops_receiver, db_responses_sender, 10);
            executor.run();
        });

        // let closure_ret_fut = || {
        //     Box::pin(async { DbResponse::None }) as Pin<Box<dyn Future<Output = DbResponse> + Send>>
        // };

        // let transaction = Box::new(closure_ret_fut) as RecvFn;
        // db_ops_sender.send(transaction).unwrap();

        // let res = db_responses_receiver.recv().unwrap();
        // assert_eq!(res, DbResponse::None);

        // let poo_file = OpenOptions::new()
        //     .read(true)
        //     .open("../poo_file_uring.txt")
        //     .unwrap();
        // let poo_fd = poo_file.as_raw_fd();

        // let pee_file = OpenOptions::new()
        //     .create(true)
        //     .truncate(true)
        //     .write(true)
        //     .open("../pee_file_uring.txt")
        //     .unwrap();
        // let pee_fd = pee_file.as_raw_fd();

        // let write_request = DbRequest::Write {
        //     file: io_uring::types::Fd(pee_fd),
        //     offset: 0,
        //     num_bytes: 11,
        //     buffer: String::from("Hello world").into_bytes().into_boxed_slice(),
        // };

        // println!("Sending write request");
        // db_ops_sender.send(write_request).unwrap();

        // let read_request = DbRequest::Read {
        //     file: io_uring::types::Fd(poo_fd),
        //     offset: 0,
        //     num_bytes: 10,
        //     buffer: vec![0; 1024].into_boxed_slice(),
        // };

        // println!("Sending read request");
        // db_ops_sender.send(read_request).unwrap();

        // println!("Waiting for response");

        // for _ in 0..2 {
        //     if let Ok(res) = db_responses_receiver.recv() {
        //         println!("Received response:");

        //         match res {
        //             DbResponse::ReadResult(result) => {
        //                 let (buffer, num_bytes) = result.unwrap();

        //                 assert_eq!(&buffer[..num_bytes], "0123456789".as_bytes());
        //                 println!("Read {} bytes: {:?}", num_bytes, &buffer[..num_bytes]);
        //             }
        //             DbResponse::WriteResult(result) => {
        //                 let (_buffer, num_bytes) = result.unwrap();

        //                 println!("Wrote {} bytes", num_bytes);
        //             }
        //         }
        //     }
        // }
    }
}
