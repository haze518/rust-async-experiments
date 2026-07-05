# Command Driver

Connect a client handle, shared command queue, driver future, and waiters.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
