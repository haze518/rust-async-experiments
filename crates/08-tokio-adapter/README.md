# Tokio Adapter

Wrap the runtime-agnostic pieces with tokio TcpStream and spawned tasks.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
