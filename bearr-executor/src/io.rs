use std::io;
use std::{
    future::Future,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::squeue;

use crate::executor::CurrentTaskContext;

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

/// # Safety
/// You're not allowed to drop the future returned until the I/O operation is complete
///
/// On success, returns the buffer and the number of bytes read.
/// On failure, returns the buffer and the error.
pub async unsafe fn read(
    file: io_uring::types::Fd,
    offset: u64,
    num_bytes: u32,
    buffer: *mut u8,
) -> io::Result<usize> {
    // assert!(buffer.len() >= num_bytes as usize);

    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::Read::new(file, buffer, num_bytes)
        .offset(offset)
        .build()
        .user_data(task_id);

    unsafe { submit_entry(entry).await };
    let err_code = wait_for_entry(task_id).await;
    if err_code >= 0 {
        Ok(err_code as usize)
    } else {
        Err(std::io::Error::from_raw_os_error(-err_code))
    }
}

/// # Safety
/// You're not allowed to drop the future returned until the I/O operation is complete
pub async unsafe fn write(
    file: io_uring::types::Fd,
    offset: u64,
    num_bytes: u32,
    buffer: *const u8,
) -> io::Result<usize> {
    // assert!(buffer.len() >= num_bytes as usize);

    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::Write::new(file, buffer, num_bytes)
        .offset(offset)
        .build()
        .user_data(task_id);

    unsafe { submit_entry(entry).await };
    let err_code = wait_for_entry(task_id).await;
    if err_code >= 0 {
        Ok(err_code as usize)
    } else {
        Err(std::io::Error::from_raw_os_error(-err_code))
    }
}

/// # Safety
/// You're not allowed to drop the future returned until the I/O operation is complete
pub async unsafe fn open(
    directory: io_uring::types::Fd,
    name: &Path,
    flags: i32,
    mode: libc::mode_t,
) -> io::Result<io_uring::types::Fd> {
    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::OpenAt::new(
        directory,
        name.as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
    )
    .flags(flags)
    .mode(mode)
    .build()
    .user_data(task_id);

    unsafe { submit_entry(entry).await }
    let err_code = wait_for_entry(task_id).await;

    if err_code >= 0 {
        Ok(io_uring::types::Fd(err_code))
    } else {
        Err(std::io::Error::from_raw_os_error(-err_code))
    }
}

/// # Safety
/// You're not allowed to drop the future returned until the I/O operation is complete
pub async unsafe fn unlink(
    directory: io_uring::types::Fd,
    name: &Path,
    flags: i32,
) -> io::Result<()> {
    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::UnlinkAt::new(
        directory,
        name.as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
    )
    .flags(flags)
    .build()
    .user_data(task_id);

    unsafe { submit_entry(entry).await }
    let err_code = wait_for_entry(task_id).await;

    if err_code >= 0 {
        assert!(err_code == 0);
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(-err_code))
    }
}

/// # Safety
/// You're not allowed to drop the future returned until the I/O operation is complete
pub async unsafe fn rename(
    old_directory: io_uring::types::Fd,
    old_name: &Path,
    new_directory: io_uring::types::Fd,
    new_name: &Path,
) -> io::Result<()> {
    let exec_ctx = CurrentTaskContext::get();

    // Being careful to not hold the exex_ctx when calling our futures
    let task_id = { exec_ctx.borrow_mut().task_id() };

    let entry = io_uring::opcode::RenameAt::new(
        old_directory,
        old_name.as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
        new_directory,
        new_name.as_os_str().as_encoded_bytes().as_ptr() as *const libc::c_char,
    )
    .build()
    .user_data(task_id);

    unsafe { submit_entry(entry).await }
    let err_code = wait_for_entry(task_id).await;

    if err_code >= 0 {
        assert!(err_code == 0);
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(-err_code))
    }
}

pub async fn register_file() {
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
