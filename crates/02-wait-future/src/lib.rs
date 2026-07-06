use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

struct CountingWaker {
    count: AtomicI32,
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        let _ = self
            .count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
enum StateKind {
    Init,
    Waiting,
    Completed,
}

struct WaitEntry {
    state: StateKind,
    waker: Option<Waker>,
}

struct WaitTable {
    entries: HashMap<i32, WaitEntry>,
}

struct StateRef(Arc<Mutex<WaitTable>>);

impl Deref for StateRef {
    type Target = Mutex<WaitTable>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct WaitFuture {
    id: i32,
    state: StateRef,
}

impl Future for WaitFuture {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let guard = &mut *self.state.0.lock().unwrap();
        let entry = guard.entries.entry(self.id).or_insert_with(|| WaitEntry {
            state: StateKind::Init,
            waker: None,
        });

        match entry.state {
            StateKind::Init | StateKind::Waiting => {
                entry.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            StateKind::Completed => Poll::Ready(Ok(())),
        }
    }
}

struct Completer {
    state: StateRef,
}

impl Completer {
    fn advance(&mut self, id: i32) {
        let waker = {
            let mut guard = self.state.lock().unwrap();
            let entry = guard.entries.get_mut(&id).unwrap();

            match entry.state {
                StateKind::Init => {
                    entry.state = StateKind::Waiting;
                    entry.waker.take()
                }
                StateKind::Waiting => {
                    entry.state = StateKind::Completed;
                    entry.waker.take()
                }
                StateKind::Completed => panic!("future is already completed"),
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub fn crate_name() -> &'static str {
    "wait-future"
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_happy() {
        let mut entries = HashMap::with_capacity(10);
        for i in 0..10 {
            entries.insert(
                i,
                WaitEntry {
                    state: StateKind::Init,
                    waker: None,
                },
            );
        }

        let state = Arc::new(Mutex::new(WaitTable { entries }));

        let mut completer = Completer {
            state: StateRef(state.clone()),
        };

        let noop_waker = Arc::new(CountingWaker {
            count: AtomicI32::new(0),
        });
        let waker = Waker::from(noop_waker.clone());
        let mut cx = Context::from_waker(&waker);

        let mut futs = Vec::with_capacity(10);
        for i in 0..10 {
            let fut = Box::pin(WaitFuture {
                id: i,
                state: StateRef(state.clone()),
            });
            futs.push(fut);
        }

        for fut in futs.iter_mut() {
            assert!(fut.as_mut().poll(&mut cx).is_pending());
        }

        assert_eq!(noop_waker.count.load(Ordering::Relaxed), 0);

        completer.advance(0);
        assert_eq!(noop_waker.count.load(Ordering::Relaxed), 1);

        assert!(futs[0].as_mut().poll(&mut cx).is_pending());

        completer.advance(0);
        assert_eq!(noop_waker.count.load(Ordering::Relaxed), 2);

        assert!(futs[0].as_mut().poll(&mut cx).is_ready());

        for fut in futs.iter_mut().skip(1) {
            assert!(fut.as_mut().poll(&mut cx).is_pending());
        }

        assert_eq!(noop_waker.count.load(Ordering::Relaxed), 2);
    }
}
