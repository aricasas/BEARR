use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use io_uring::{CompletionQueue, SubmissionQueue};

struct FileReadFuture<'a> {
    s_queue: SubmissionQueue<'a>,
    c_queue: CompletionQueue<'a>,
    file: io_uring::types::Fixed,
    offset: u64,
    num_bytes: u32,
    submitted: bool,
    buffer: Vec<u8>,
    id: u64,
}

impl<'a> Future for FileReadFuture<'a> {
    type Output = Result<Vec<u8>, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fake_self = self.get_mut();

        if !fake_self.submitted {
            let mut entry = io_uring::opcode::Read::new(
                fake_self.file,
                fake_self.buffer.as_mut_ptr(),
                fake_self.num_bytes,
            )
            .build();
            entry.set_user_data(fake_self.id);

            // TODO we are using this incorrectyl for now
            unsafe { fake_self.s_queue.push(&entry) };

            spawn_thread(||{
// check completion queue until find corresponding entry
cx.waker().wake();

            })
            fake_self.submitted = true;
            return Poll::Pending;
        } else {
            fake_self.c_queue.sync();
            let x = fake_self
                .c_queue
                .find(|entry| entry.user_data() == fake_self.id)
                .map(|entry| entry.result());
            match x {
                Some(y) => {
                    if y == 0 {
                        eprintln!("Everything good");
                        return Poll::Ready(Ok(fake_self.buffer.clone()));
                    } else {
                        eprintln!("IO error");
                        return Poll::Ready(Err(()));
                    }
                }
                None => return Poll::Pending,
            }
        }
    }
}

// SAFETY: You're not allowed to drop the `FileReadFuture` struct until the I/O operation is complete
unsafe fn read<'a>(
    s_queue: SubmissionQueue<'a>,
    c_queue: CompletionQueue<'a>,
    file: io_uring::types::Fixed,
    offset: u64,
    num_bytes: u32,
) -> FileReadFuture<'a> {
    FileReadFuture {
        file,
        offset,
        num_bytes,
        s_queue,
        c_queue,
        buffer: Vec::with_capacity(num_bytes as usize),
        submitted: false,
        id: 15,
    }
}

async fn btree_read() {
    let first_block = read(s_queue, "poop.basetas", 0, 4096).await.unwrap();
    first_block[5] = 0;
}

fn main() {}

fn executor_main() {
    let active_tasks = Vec::new();
    let inactive_tasks = Vec::new();

    stuct waker;
    wake(){
        //move task from inactive to active
    }

    loop {
        for task in active_tasks {
            match task.poll() {
                Poll::Pending => {
                    // move task to inactive_tasks
                }
                Poll::Ready(x) => {
                    // do something with it
                }
            }
        }

        // Check completion queue;
        // for items in completion queue, put their corresponding tasks in active_tasks
    }
}
