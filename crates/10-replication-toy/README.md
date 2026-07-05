# Replication Toy

Build a small streaming protocol with LSNs, keepalives, ACKs, reconnect, and shutdown.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
