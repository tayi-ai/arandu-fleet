# Changelog

Everything worth knowing about a release of Arandu Fleet is recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A published module version is immutable: Go serves it from the proxy forever, so
a release is corrected by another release and never by moving a tag.

## [Unreleased]

## [0.2.0] - 2026-09-13

### Added

- The existing record API is now explicitly recorded with its five guarded
  actions: `FleetRecordView`, `FleetRecordList`, `FleetRecordCreate`,
  `FleetRecordUpdate` and `FleetRecordDelete`. Its schema remains migration
  `20260823_0001_create_fleets`.
- Durable node process identity, generation fencing and reconciliation after an
  agent restart. A restored process is trusted only when its PID, Linux start
  time and executable still match; an unprovable process is reported as
  `unknown`.
- Idempotent submission and cancellation for the same fenced job, graceful
  process-group termination followed by a bounded forced stop, and persisted
  final output retrieved through `GET /jobs/{id}/result` with a SHA-256 digest.
- Exact-node status, dispatch, cancellation and release operations so one
  execution can reserve disjoint RPC and coordinator phases without releasing
  the cluster between them.

### Changed

- `Job`, `NodeRun` and `Run` now carry generation and runtime identity fields.
  External unkeyed composite literals must move to keyed fields when upgrading.
- Result logs are limited to 8 MiB at the agent and to a bounded escaped JSON
  response at the HTTP client.

[Unreleased]: https://github.com/tayi-ai/arandu-fleet/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/tayi-ai/arandu-fleet/compare/v0.1.2...v0.2.0
