# Upgrade Guide

## Unreleased

## v0.3.0

`Job` and persisted `NodeRun` now also carry `ModelRecipe` and `ModelDigest`.
Callers using keyed literals may omit them only for actions that do not select a
model. Inference jobs should set both values from the admitted recipe. A retry
with a different model identity is rejected instead of adopting the prior
process.

## v0.2.0

`Job`, `NodeRun` and `Run` gained fields that fence a process by generation and
identify its runtime. Code using unkeyed composite literals for these types must
replace positional values with keyed fields. Existing keyed literals continue
to compile; omitted generation fields retain their zero value for the original
diagnostics and collective actions.

Agents now persist `current.json` beside their run logs. Keep that directory on
persistent storage if a worker process must reconcile an in-flight job after a
restart. A process whose Linux PID, start time and executable cannot be proven
is reported as `unknown` and is never adopted silently.
