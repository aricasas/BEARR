use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Waker},
};

use crate::executor::executor::{CurrentTaskContext, TaskId};

pub struct Mutex<T> {
    queue: std::sync::Mutex<VecDeque<(TaskId, Waker)>>,
    value: UnsafeCell<T>,
    is_held: AtomicBool,
}

unsafe impl<T> Send for Mutex<T> {}
unsafe impl<T> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            queue: std::sync::Mutex::new(VecDeque::new()),
            value: UnsafeCell::new(value),
            is_held: AtomicBool::new(false),
        }
    }

    pub async fn lock(&'_ self) -> LockGuard<'_, T> {
        Lock::Init { mutex: self }.await;
        LockGuard { mutex: self }
    }
}

pub struct LockGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        let queue = self.mutex.queue.lock().unwrap();
        // TODO: Check what ordering we need
        self.mutex.is_held.store(false, Ordering::SeqCst);

        if let Some((_, waker)) = queue.front() {
            waker.wake_by_ref();
        }
    }
}

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.value.get() }
    }
}

impl<'a, T> DerefMut for LockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.value.get() }
    }
}

enum Lock<'a, T> {
    Init { mutex: &'a Mutex<T> },
    Queued { mutex: &'a Mutex<T> },
    Done,
}
impl<'a, T> Future for Lock<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this {
            Lock::Init { mutex } => {
                if mutex.is_held.swap(true, Ordering::SeqCst) {
                    let mut queue = mutex.queue.lock().unwrap();

                    // Lock was already held, queue ourselves
                    let waker = cx.waker();
                    let task_id = CurrentTaskContext::get().borrow().task_id();

                    queue.push_back((task_id, waker.clone()));
                    *this = Lock::Queued { mutex };

                    Poll::Pending
                } else {
                    // We obtained the lock
                    *this = Lock::Done;

                    Poll::Ready(())
                }
            }
            Lock::Queued { mutex } => {
                if mutex.is_held.swap(true, Ordering::SeqCst) {
                    // Didn't acquire it, do nothing
                    *this = Lock::Queued { mutex };
                    Poll::Pending
                } else {
                    // We obtained the lock, remove ourselves from the queue
                    let task_id = CurrentTaskContext::get().borrow().task_id();

                    let mut queue = mutex.queue.lock().unwrap();
                    if let Some(pos) = queue.iter().position(|(id, _)| *id == task_id) {
                        queue.remove(pos);
                    }
                    *this = Lock::Done;

                    Poll::Ready(())
                }
            }
            Lock::Done => todo!(),
        }
    }
}
