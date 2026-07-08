use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    task::{Poll, Wake, Waker},
};

struct State {
    commands: VecDeque<DriverCommand>,
    waker: Option<Waker>,
    closed: bool,
}

struct StateRef(Arc<Mutex<State>>);

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: std::sync::Arc<Self>) {}
}

#[derive(Debug, Copy, Clone)]
enum DriverCommand {
    Close,
    Ping(usize),
}

struct Driver {
    state: StateRef,
    completer: Completer,
}

#[derive(Debug)]
struct WaitTable {
    entries: HashMap<usize, FutureState>,
}

#[derive(Debug)]
struct WaitTableRef(Arc<Mutex<WaitTable>>);

struct Completer {
    inner: WaitTableRef,
}

impl Completer {
    fn advance(&self, id: usize) {
        let waker = {
            let mut guard = self.inner.0.lock().unwrap();

            let entry = guard.entries.get_mut(&id);
            if let Some(entry) = entry {
                entry.finished = true;
                entry.waker.take()
            } else {
                None
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn complete(&self) {
        let wakers = {
            let mut guard = self.inner.0.lock().unwrap();
            guard
                .entries
                .values_mut()
                .filter_map(|entry| {
                    entry.finished = true;
                    entry.waker.take()
                })
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

#[derive(Debug)]
struct FutureState {
    waker: Option<Waker>,
    finished: bool,
}

#[derive(Debug)]
struct WaitFuture {
    id: usize,
    inner: WaitTableRef,
}

impl Future for WaitFuture {
    type Output = Result<(), io::Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let guard = &mut self.inner.0.lock().unwrap();

        let entry = guard.entries.get_mut(&self.id).unwrap();
        if entry.finished {
            Poll::Ready(Ok(()))
        } else {
            entry.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for Driver {
    type Output = Result<(), io::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            let command = {
                let mut guard = self.state.0.lock().unwrap();
                guard.commands.pop_front()
            };

            match command {
                Some(DriverCommand::Ping(id)) => {
                    self.completer.advance(id);
                    continue;
                }
                Some(DriverCommand::Close) => {
                    self.completer.complete();
                    return Poll::Ready(Ok(()));
                }
                None => {
                    let mut guard = self.state.0.lock().unwrap();
                    guard.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
        }
    }
}

struct ClientHandle {
    state: StateRef,
    wait_table: WaitTableRef,
    next: AtomicUsize,
}

impl ClientHandle {
    fn ping(&self) -> Result<WaitFuture, io::Error> {
        let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let waker = {
            let mut state = self.state.0.lock().unwrap();

            if state.closed {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "driver is closed",
                ));
            }

            {
                let mut table = self.wait_table.0.lock().unwrap();
                table.entries.insert(
                    id,
                    FutureState {
                        waker: None,
                        finished: false,
                    },
                );
            }

            state.commands.push_back(DriverCommand::Ping(id));
            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }

        Ok(WaitFuture {
            id,
            inner: WaitTableRef(self.wait_table.0.clone()),
        })
    }

    fn close(&self) {
        let waker = {
            let mut guard = self.state.0.lock().unwrap();

            if guard.closed {
                return;
            }

            guard.closed = true;

            guard.commands.push_back(DriverCommand::Close);
            guard.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub fn crate_name() -> &'static str {
    "command-driver"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Waker},
    };

    fn new_pair() -> (ClientHandle, Driver) {
        let state = StateRef(Arc::new(Mutex::new(State {
            commands: VecDeque::new(),
            waker: None,
            closed: false,
        })));

        let wait_table = WaitTableRef(Arc::new(Mutex::new(WaitTable {
            entries: HashMap::new(),
        })));

        let client = ClientHandle {
            state: StateRef(state.0.clone()),
            wait_table: WaitTableRef(wait_table.0.clone()),
            next: AtomicUsize::new(0),
        };

        let driver = Driver {
            state,
            completer: Completer {
                inner: WaitTableRef(wait_table.0.clone()),
            },
        };

        (client, driver)
    }

    fn noop_cx() -> Context<'static> {
        let waker = Waker::from(Arc::new(NoopWaker));
        Context::from_waker(Box::leak(Box::new(waker)))
    }

    #[test]
    fn ping_is_pending_before_driver_runs() {
        let (client, _driver) = new_pair();
        let mut fut = client.ping().unwrap();

        let mut cx = noop_cx();

        let res = Pin::new(&mut fut).poll(&mut cx);

        assert!(matches!(res, Poll::Pending));
    }

    #[test]
    fn ping_is_ready_after_driver_runs() {
        let (client, mut driver) = new_pair();
        let mut fut = client.ping().unwrap();

        let mut cx = noop_cx();

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        assert!(matches!(Pin::new(&mut driver).poll(&mut cx), Poll::Pending));

        assert!(matches!(
            Pin::new(&mut fut).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn close_completes_driver() {
        let (client, mut driver) = new_pair();

        client.close();

        let mut cx = noop_cx();

        assert!(matches!(
            Pin::new(&mut driver).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn ping_after_close_returns_error() {
        let (client, _driver) = new_pair();

        client.close();

        let err = client.ping().unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn close_completes_pending_waiter() {
        let (client, mut driver) = new_pair();

        let mut fut = client.ping().unwrap();
        client.close();

        let mut cx = noop_cx();

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        assert!(matches!(
            Pin::new(&mut driver).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));

        assert!(matches!(
            Pin::new(&mut fut).poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }
}
