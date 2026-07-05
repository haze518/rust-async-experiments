use std::io;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

enum StateKind {
    Init,
    Waiting,
    Completed,
}

struct State {
    waker: Option<Waker>,
    inner: StateKind,
}

struct StateRef(Arc<Mutex<State>>);

impl Deref for StateRef {
    type Target = Mutex<State>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Test(StateRef);

impl Future for Test {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = &mut *self.0.lock().unwrap();

        match state.inner {
            StateKind::Init | StateKind::Waiting => {
                println!("ready or pending state");
                state.waker = Some(cx.waker().clone());
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
    fn advance(&mut self) {
        let waker = {
            let mut guard = self.state.lock().unwrap();

            match guard.inner {
                StateKind::Init => {
                    guard.inner = StateKind::Waiting;
                    guard.waker.take()
                }
                StateKind::Waiting => {
                    guard.inner = StateKind::Completed;
                    guard.waker.take()
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
    "manual-future"
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;

    #[test]
    fn test_happy() {
        let state = Arc::new(Mutex::new(State {
            waker: None,
            inner: StateKind::Init,
        }));

        let state_ref = StateRef(state.clone());
        let mut completer = Completer {
            state: StateRef(state.clone()),
        };

        let test = Test(state_ref);
        let mut fut = pin!(test);

        let waker = Arc::new(NoopWaker {}).into();
        let mut cx = Context::from_waker(&waker);

        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(_) => break,
                Poll::Pending => completer.advance(),
            }
        }
    }
}
