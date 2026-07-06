use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};

struct TaskWaker {
    task_id: i32,
    state: ExecutorState,
}

impl Wake for TaskWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.state.push(self.task_id);
    }
}

enum StateKind {
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

struct FutStateRef(Arc<Mutex<WaitTable>>);

struct Task {
    id: i32,
    fut: Pin<Box<WaitFuture>>,
}

struct WaitFuture {
    id: i32,
    state: FutStateRef,
}

impl Future for WaitFuture {
    type Output = Result<(), std::io::Error>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let guard = &mut self.state.0.lock().unwrap();

        let entry = guard.entries.entry(self.id).or_insert_with(|| WaitEntry {
            state: StateKind::Waiting,
            waker: None,
        });

        match entry.state {
            StateKind::Waiting => {
                entry.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            StateKind::Completed => Poll::Ready(Ok(())),
        }
    }
}

struct Completer {
    state: FutStateRef,
}

impl Completer {
    fn advance(&self, id: i32) {
        let waker = {
            let mut guard = self.state.0.lock().unwrap();
            let entry = guard.entries.get_mut(&id).unwrap();

            match entry.state {
                StateKind::Waiting => {
                    entry.state = StateKind::Completed;
                    entry.waker.take()
                }
                StateKind::Completed => panic!("future already completed"),
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct ExecutorStateRef {
    queue: VecDeque<i32>,
    tasks: HashMap<i32, Task>,
}

struct ExecutorState(Arc<Mutex<ExecutorStateRef>>);

impl Clone for ExecutorState {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl ExecutorState {
    fn push(&self, task_id: i32) {
        self.0.lock().unwrap().queue.push_back(task_id);
    }
}

struct Executor {
    state: ExecutorState,
}

impl Executor {
    fn run(&mut self) {
        loop {
            let id = {
                let mut guard = self.state.0.lock().unwrap();
                guard.queue.pop_front()
            };

            let Some(id) = id else {
                break;
            };

            let task = {
                let mut guard = self.state.0.lock().unwrap();
                guard.tasks.remove(&id)
            };

            let Some(mut task) = task else {
                continue;
            };

            let waker = Waker::from(Arc::new(TaskWaker {
                task_id: id,
                state: self.state.clone(),
            }));
            let mut cx = Context::from_waker(&waker);

            match task.fut.as_mut().poll(&mut cx) {
                Poll::Ready(_) => {}
                Poll::Pending => {
                    let mut guard = self.state.0.lock().unwrap();
                    guard.tasks.insert(id, task);
                }
            }
        }
    }
}

pub fn crate_name() -> &'static str {
    "mini-executor"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_happy() {
        let mut entries = HashMap::with_capacity(10);
        for i in 0..10 {
            entries.insert(
                i,
                WaitEntry {
                    state: StateKind::Waiting,
                    waker: None,
                },
            );
        }
        let fut_state = FutStateRef(Arc::new(Mutex::new(WaitTable { entries })));
        let completer = Completer {
            state: FutStateRef(fut_state.0.clone()),
        };

        let exec_state = ExecutorState(Arc::new(Mutex::new(ExecutorStateRef {
            queue: VecDeque::new(),
            tasks: HashMap::new(),
        })));

        {
            let mut guard = exec_state.0.lock().unwrap();
            for i in 0..10 {
                let task = Task {
                    id: i,
                    fut: Box::pin(WaitFuture {
                        id: i,
                        state: FutStateRef(fut_state.0.clone()),
                    }),
                };
                guard.tasks.insert(i, task);
                guard.queue.push_back(i);
            }
        }

        let mut executor = Executor {
            state: exec_state.clone(),
        };

        executor.run();
        {
            let guard = exec_state.0.lock().unwrap();
            assert!(guard.queue.is_empty());
            assert_eq!(guard.tasks.len(), 10);
        }

        completer.advance(3);

        {
            let guard = exec_state.0.lock().unwrap();
            assert_eq!(guard.queue.len(), 1);
            assert_eq!(guard.queue[0], 3);
        }

        executor.run();

        {
            let guard = exec_state.0.lock().unwrap();
            assert!(guard.queue.is_empty());
            assert_eq!(guard.tasks.len(), 9);
            assert!(!guard.tasks.contains_key(&3));
        }
    }
}
