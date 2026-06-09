use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
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

// State encoding (AtomicU64):
//   bits [0..61]  active reader count
//   bit  62       writer is holding the lock
//   bit  63       at least one writer is queued (blocks new fast-path readers)
//
// Fast path (no OS mutex):
//   read-acquire  : CAS  state += 1  when bits 62-63 are both clear
//   read-release  : fetch_sub(1); only acquire wait_queue if count hits 0
//   write-acquire : CAS  state  0 -> WRITER_ACTIVE
//
// Slow path (OS mutex for wait_queue):
//   entered when the fast path CAS fails or WRITER_WAITING is set

const WRITER_ACTIVE: u64 = 1 << 62;
const WRITER_WAITING: u64 = 1 << 63;
const READER_MASK: u64 = WRITER_ACTIVE - 1; // low 62 bits

pub struct RwLock<T> {
    value: UnsafeCell<T>,
    state: AtomicU64,
    wait_queue: std::sync::Mutex<VecDeque<(bool, TaskId, Waker)>>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
            state: AtomicU64::new(0),
            wait_queue: std::sync::Mutex::new(VecDeque::new()),
        }
    }

    pub async fn read(&self) -> ReadGuard<'_, T> {
        RwRead::Init { lock: self }.await;
        ReadGuard { lock: self }
    }

    pub async fn write(&self) -> WriteGuard<'_, T> {
        RwWrite::Init { lock: self }.await;
        WriteGuard { lock: self }
    }
}

pub struct ReadGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        let old = self.lock.state.fetch_sub(1, Ordering::AcqRel);
        // Only the last departing reader needs to wake a queued writer.
        if (old - 1) & READER_MASK == 0 {
            let queue = self.lock.wait_queue.lock().unwrap();
            if let Some((_, _, waker)) = queue.front() {
                waker.wake_by_ref();
            }
        }
    }
}

impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

pub struct WriteGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut queue = self.lock.wait_queue.lock().unwrap();
        let more_writers = queue.iter().any(|(is_w, _, _)| *is_w);

        if more_writers {
            // Clear WRITER_ACTIVE but keep WRITER_WAITING
            self.lock.state.fetch_and(!WRITER_ACTIVE, Ordering::Release);
        } else {
            // No more writers queued
            self.lock
                .state
                .fetch_and(!(WRITER_ACTIVE | WRITER_WAITING), Ordering::Release);
        }

        // Wake one writer or all consecutive readers at the front of the queue
        match queue.front() {
            Some((true, _, waker)) => waker.wake_by_ref(),
            Some((false, _, _)) => {
                for (is_w, _, waker) in queue.iter() {
                    if *is_w {
                        break;
                    }
                    waker.wake_by_ref();
                }
            }
            None => {}
        }
    }
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.value.get() }
    }
}

enum RwRead<'a, T> {
    Init { lock: &'a RwLock<T> },
    Queued { lock: &'a RwLock<T> },
    Done,
}

impl<'a, T> Future for RwRead<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this {
            RwRead::Init { lock } => {
                // Fast path: CAS reader_count + 1 if no writer is active or waiting.
                let mut s = lock.state.load(Ordering::Acquire);

                loop {
                    if s & (WRITER_ACTIVE | WRITER_WAITING) == 0 {
                        let ss = lock.state.compare_exchange(
                            s,
                            s + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );

                        match ss {
                            Ok(_) => {
                                *this = RwRead::Done;
                                return Poll::Ready(());
                            }
                            Err(new_s) => s = new_s,
                        }
                    } else {
                        break;
                    }
                }

                // Slow path: enqueue.
                let task_id = CurrentTaskContext::get().borrow().task_id();
                let mut queue = lock.wait_queue.lock().unwrap();
                queue.push_back((false, task_id, cx.waker().clone()));
                *this = RwRead::Queued { lock };
                Poll::Pending
            }
            RwRead::Queued { lock } => {
                let s = lock.state.load(Ordering::Acquire);
                if s & WRITER_ACTIVE != 0 {
                    return Poll::Pending;
                }
                // Check whether a writer is queued ahead of us
                let task_id = CurrentTaskContext::get().borrow().task_id();
                let mut queue = lock.wait_queue.lock().unwrap();
                let my_pos = queue.iter().position(|(_, id, _)| *id == task_id);
                let writer_before_us =
                    my_pos.is_some_and(|pos| queue.iter().take(pos).any(|(is_w, _, _)| *is_w));
                if writer_before_us {
                    return Poll::Pending;
                }
                // Acquire: increment reader count and leave the queue
                lock.state.fetch_add(1, Ordering::AcqRel);
                if let Some(pos) = my_pos {
                    queue.remove(pos);
                }
                *this = RwRead::Done;
                Poll::Ready(())
            }
            RwRead::Done => todo!(),
        }
    }
}

enum RwWrite<'a, T> {
    Init { lock: &'a RwLock<T> },
    Queued { lock: &'a RwLock<T> },
    Done,
}

impl<'a, T> Future for RwWrite<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this {
            RwWrite::Init { lock } => {
                // Fast path: CAS 0 -> WRITER_ACTIVE when lock is completely free.
                if lock
                    .state
                    .compare_exchange(0, WRITER_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    *this = RwWrite::Done;
                    return Poll::Ready(());
                }
                // Mark writer as waiting so new fast-path readers defer to the queue.
                lock.state.fetch_or(WRITER_WAITING, Ordering::AcqRel);
                let task_id = CurrentTaskContext::get().borrow().task_id();
                let mut queue = lock.wait_queue.lock().unwrap();
                queue.push_back((true, task_id, cx.waker().clone()));
                *this = RwWrite::Queued { lock };
                Poll::Pending
            }
            RwWrite::Queued { lock } => {
                let s = lock.state.load(Ordering::Acquire);
                if s & (READER_MASK | WRITER_ACTIVE) != 0 {
                    return Poll::Pending;
                }
                // Grab the write lock.
                let task_id = CurrentTaskContext::get().borrow().task_id();
                let mut queue = lock.wait_queue.lock().unwrap();
                if let Some(pos) = queue.iter().position(|(_, id, _)| *id == task_id) {
                    assert_eq!(pos, 0); // TODO: enforce this with logic instead of assertion
                    queue.remove(pos);
                }
                let more_writers = queue.iter().any(|(is_w, _, _)| *is_w);
                // Keep WRITER_WAITING set only while more writers remain.
                lock.state.store(
                    WRITER_ACTIVE | if more_writers { WRITER_WAITING } else { 0 },
                    Ordering::Release,
                );
                *this = RwWrite::Done;
                Poll::Ready(())
            }
            RwWrite::Done => todo!(),
        }
    }
}
