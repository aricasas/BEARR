use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use io_uring::{CompletionQueue, SubmissionQueue, squeue};

use crate::DbResponse;

struct SubmissionQueueWait<'a> {
    s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
    entry: squeue::Entry,
    registry: Arc<Mutex<VecDeque<Waker>>>,
}

impl<'a> Future for SubmissionQueueWait<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match unsafe { this.s_queue.lock().unwrap().push(&this.entry) } {
            Ok(()) => Poll::Ready(()),
            Err(_) => {
                this.registry.lock().unwrap().push_back(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// SAFETY: You have to make sure the resources in the Entry are valid until
/// the I/O operation is complete
unsafe fn submit_entry<'a>(
    s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
    entry: squeue::Entry,
    registry: Arc<Mutex<VecDeque<Waker>>>,
) -> SubmissionQueueWait<'a> {
    SubmissionQueueWait {
        s_queue,
        entry,
        registry,
    }
}

struct CompletionQueueWait<'a> {
    c_queue: Arc<Mutex<CompletionQueue<'a>>>,
    id: u64,
    registry: Arc<Mutex<HashMap<u64, Waker>>>,
    completion_codes: Arc<Mutex<HashMap<u64, i32>>>,
}
impl<'a> Future for CompletionQueueWait<'a> {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let codes = this.completion_codes.lock().unwrap();
        let res = codes.get(&this.id).copied();

        if let Some(code) = res {
            // Remove from registry if it was there
            this.registry.lock().unwrap().remove(&this.id);
            Poll::Ready(code)
        } else {
            // Register the waker
            this.registry
                .lock()
                .unwrap()
                .insert(this.id, cx.waker().clone());
            Poll::Pending
        }
    }
}

fn wait_for_entry<'a>(
    c_queue: Arc<Mutex<CompletionQueue<'a>>>,
    id: u64,
    registry: Arc<Mutex<HashMap<u64, Waker>>>,
    completion_codes: Arc<Mutex<HashMap<u64, i32>>>,
) -> CompletionQueueWait<'a> {
    CompletionQueueWait {
        c_queue,
        id,
        registry,
        completion_codes,
    }
}

/// SAFETY: You're not allowed to drop the future returned until the I/O operation is complete
///
/// On success, returns the buffer and the number of bytes read.
/// On failure, returns the buffer and the error.
pub async unsafe fn read<'a>(
    s_queue: Arc<Mutex<SubmissionQueue<'a>>>,
    c_queue: Arc<Mutex<CompletionQueue<'a>>>,
    file: io_uring::types::Fd,
    offset: u64,
    num_bytes: u32,
    mut buffer: Box<[u8]>,

    // TODO: obtain these from thread local that is set
    // by the executor instead of passing them in as arguments
    id: u64,
    submission_registry: Arc<Mutex<VecDeque<Waker>>>,
    completion_registry: Arc<Mutex<HashMap<u64, Waker>>>,
    completion_codes: Arc<Mutex<HashMap<u64, i32>>>,
) -> DbResponse {
    assert!(buffer.len() >= num_bytes as usize);

    let entry = io_uring::opcode::Read::new(file, buffer.as_mut_ptr(), num_bytes)
        .offset(offset)
        .build()
        .user_data(id);

    unsafe { submit_entry(s_queue, entry, submission_registry).await };
    let err_code = wait_for_entry(c_queue, id, completion_registry, completion_codes).await;
    if err_code >= 0 {
        DbResponse::ReadResult(Ok((buffer, err_code as usize)))
    } else {
        DbResponse::ReadResult(Err((buffer, std::io::Error::from_raw_os_error(-err_code))))
    }
}
