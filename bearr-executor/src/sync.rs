use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll, Waker},
};

pub struct Mutex<T> {
    queue: std::sync::Mutex<VecDeque<Waker>>,
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
        Lock { mutex: self }.await;
        LockGuard { mutex: self }
    }
}

pub struct LockGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<'a, T> Drop for LockGuard<'a, T> {
    fn drop(&mut self) {
        let mut queue = self.mutex.queue.lock().unwrap();
        // TODO: Check what ordering we need
        self.mutex.is_held.store(false, Ordering::SeqCst);

        if let Some(waker) = queue.pop_front() {
            waker.wake();
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

struct Lock<'a, T> {
    mutex: &'a Mutex<T>,
}
impl<'a, T> Future for Lock<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut queue = this.mutex.queue.lock().unwrap();

        // TODO: Check what ordering we need
        if this.mutex.is_held.swap(true, Ordering::SeqCst) {
            // Lock was already held, queue ourselves
            let waker = cx.waker();

            queue.push_back(waker.clone());
            Poll::Pending
        } else {
            // We obtained the lock
            Poll::Ready(())
        }
    }
}
