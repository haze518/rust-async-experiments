# Backpressure Cancellation

Handle bounded queues, dropped waiters, shutdown, and slow consumers.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
