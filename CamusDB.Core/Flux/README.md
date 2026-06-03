# Flux

A minimal, allocation-light state machine used to drive multi-step database operations.

`FluxMachine<TSteps, TState>` maps enum values to handler functions (sync or async). Each handler returns a `FluxAction` that tells the machine to continue to the next step, jump to a specific step, or abort. If the machine is aborted an optional abort handler runs for cleanup.

This pattern keeps complex, sequential operations (e.g. insert: validate → acquire lock → write row → update indexes → commit) readable as an ordered list of named steps rather than a deeply nested call chain.
