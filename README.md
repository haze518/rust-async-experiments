# rust-async-experiments

A small workspace for practicing async Rust from the bottom up: manual futures, wakers, driver futures, runtime-agnostic transports, and Sans-IO protocol cores.

## Exercises

1. `01-manual-future` - implement futures by hand and get comfortable with `Poll`, `Context`, and `Pin`.
2. `02-wait-future` - build a `WaitFuture` backed by shared waiter state.
3. `03-mini-executor` - build a tiny executor and a custom `Waker`.
4. `04-command-driver` - model a client handle that wakes a background driver future.
5. `05-mock-transport` - handle `Pending`, partial reads, partial writes, and flush.
6. `06-sans-io-core` - parse a small line protocol without depending on any runtime.
7. `07-runtime-agnostic-driver` - connect core, transport, commands, waiters, and events.
8. `08-tokio-adapter` - add a tokio TCP adapter and a tiny test server.
9. `09-backpressure-cancellation` - add bounded queues, cancellation, shutdown, and slow-consumer behavior.
10. `10-replication-toy` - build a small logical-replication-like streaming protocol.

Run everything with:

```sh
cargo check --workspace
```
