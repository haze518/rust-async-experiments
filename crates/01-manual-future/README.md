# Manual Future

Write small futures by hand and make the poll contract feel concrete.

## Checklist

- Define the states before writing behavior.
- Decide where readiness is stored.
- Register or update the waker before returning `Poll::Pending`.
- Add small tests for both ready and pending paths.
