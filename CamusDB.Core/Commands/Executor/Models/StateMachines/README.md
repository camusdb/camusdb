# StateMachines

Flux state and step types for each multi-step DML and DDL operation. Every operation that touches storage in more than one place is driven by a `FluxMachine<TSteps, TState>` so each phase is an explicit, named step.

Each operation has two types:
- `*FluxSteps` — an enum listing the steps in execution order.
- `*FluxState` — a record/class holding all mutable context the steps share (ticket, open table/database descriptors, acquired locks, intermediate results, etc.).

| Operation | Steps enum | State class |
|-----------|-----------|-------------|
| INSERT | `InsertFluxSteps` | `InsertFluxState` |
| UPDATE | `UpdateFluxSteps` | `UpdateFluxState` |
| DELETE | `DeleteFluxSteps` | `DeleteFluxState` |
| ADD COLUMN | `AlterColumnFluxSteps` | `AlterColumnFluxState`, `AlterColumnFluxIndexState` |
| ADD INDEX | `AddIndexFluxSteps` | `AddIndexFluxState` |
| DROP INDEX | `DropIndexFluxSteps` | `DropIndexFluxState` |
| ALTER INDEX | _(reuses ADD/DROP)_ | `AlterIndexFluxIndexState` |
