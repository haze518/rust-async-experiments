# Mock Transport

Model partial reads, partial writes, Pending, flush, and write offsets.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
