use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{register, squeue};

use crate::{DbResponse, executor::CurrentTaskContext};

pub type IoId = u64;

struct SubmissionQueueWait {
    entry: squeue::Entry,
}

impl Future for SubmissionQueueWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let binding = CurrentTaskContext::get();
        let exec_ctx = &mut *binding.borrow_mut();
        let res;
        {
            let mut s_queue = exec_ctx.ring().submission();
            res = unsafe { s_queue.push(&this.entry) };
        }
        match res {
            Ok(()) => Poll::Ready(()),
            Err(_) => {
                exec_ctx.register_submission_wait(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// SAFETY: You have to make sure the resources in the Entry are valid until
/// the I/O operation is complete
unsafe fn submit_entry(entry: squeue::Entry) -> SubmissionQueueWait {
    SubmissionQueueWait { entry }
}

struct CompletionQueueWait {
    id: IoId,
}
impl Future for CompletionQueueWait {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let binding = CurrentTaskContext::get();
        let mut exec_ctx = binding.borrow_mut();

        let res = exec_ctx.consume_completion(this.id);

        if let Some(code) = res {
            // Remove from registry if it was there
            Poll::Ready(code)
        } else {
            // Register the waker
            exec_ctx.register_completion_wait(this.id, cx.waker().clone());
            Poll::Pending
        }
    }
}

fn wait_for_entry(id: IoId) -> CompletionQueueWait {
    CompletionQueueWait { id }
}

/// SAFETY: You're not allowed to drop the future returned until the I/O operation is complete
///
/// On success, returns the buffer and the number of bytes read.
/// On failure, returns the buffer and the error.
pub async unsafe fn read(
    file: io_uring::types::Fd,
    offset: u64,
    num_bytes: u32,
    mut buffer: Box<[u8]>,
) -> DbResponse {
    assert!(buffer.len() >= num_bytes as usize);

    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::Read::new(file, buffer.as_mut_ptr(), num_bytes)
        .offset(offset)
        .build()
        .user_data(task_id);

    unsafe { submit_entry(entry).await };
    let err_code = wait_for_entry(task_id).await;
    if err_code >= 0 {
        DbResponse::ReadResult(Ok((buffer, err_code as usize)))
    } else {
        DbResponse::ReadResult(Err((buffer, std::io::Error::from_raw_os_error(-err_code))))
    }
}

pub async unsafe fn write(
    file: io_uring::types::Fd,
    offset: u64,
    num_bytes: u32,
    mut buffer: Box<[u8]>,
) -> DbResponse {
    assert!(buffer.len() >= num_bytes as usize);

    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::Write::new(file, buffer.as_mut_ptr(), num_bytes)
        .offset(offset)
        .build()
        .user_data(task_id);

    unsafe { submit_entry(entry).await };
    let err_code = wait_for_entry(task_id).await;
    if err_code >= 0 {
        DbResponse::WriteResult(Ok((buffer, err_code as usize)))
    } else {
        DbResponse::WriteResult(Err((buffer, std::io::Error::from_raw_os_error(-err_code))))
    }
}

pub async fn register_files() {
    unimplemented!()
}

pub async fn unregister_files() {
    unimplemented!()
}

pub async fn register_buffers() {
    unimplemented!()
}

pub async fn unregister_buffers() {
    unimplemented!()
}
