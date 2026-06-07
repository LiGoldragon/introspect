# skills - introspect

*Per-repo agent guide.*

## Checkpoint - read before editing

Before changing code in this repo, read:

- `~/primary/skills/operator.md`
- `~/primary/skills/kameo.md`
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/rust-discipline.md`
- `~/primary/skills/architectural-truth-tests.md`
- `~/primary/skills/nix-discipline.md`
- `~/primary/skills/subscription-lifecycle.md`
- `~/primary/skills/push-not-pull.md`
- this repo's `ARCHITECTURE.md`
- `signal-introspect/ARCHITECTURE.md`

## What this repo owns

- `introspect-daemon`
- `introspect` CLI
- Runtime fan-out to component daemons over Signal.
- Local observation audit state in `introspect.sema`, opened through
  `sema-engine`.
- `introspect-daemon` starts from one signal-encoded rkyv
  `IntrospectDaemonConfiguration` file. Inline NOTA and `.nota`
  startup files are rejected.
- NOTA projection at the CLI edge.

## What this repo does not own

- Other components' database files.
- Router, terminal, manager, harness, message, system, or mind policy.
- Component observation record definitions.

Live introspection asks component daemons. It never opens their databases.

## Peer-query and subscription discipline

`ManagerClient`, `RouterClient`, and `TerminalClient` each own
exactly one peer relationship. Each client either opens a typed
Subscribe stream against its peer (push subscription) or sends a
typed Match request (one-shot query). It never polls; it never
re-asks on a timer.

Subscription open returns a typed snapshot reply carrying the
per-stream token and a sequence pointer; subsequent deltas push
as typed events; close is a typed Retract request; the final ack
is a typed reply event. The full lifecycle is named in
`~/primary/skills/subscription-lifecycle.md`.

When a peer client encodes a Match request (e.g. `RouterRequest::Summary`),
it sends one typed Signal frame, parses the typed reply, and
composes the result into the carrier record (`PrototypeWitness`,
`ComponentSnapshot`, etc.) — using `Some(state)` when the peer
answered, `None` when the peer socket is not configured or the
peer daemon has not yet shipped that contract operation.

Delivery-trace observations are keyed by
`signal_introspect::DeliveryTraceKey`. The first three fields
(`engine`, `message_identifier`, `originator`) join one delivery chain;
`hop_index` orders the events. Tests should insert events out of order
and query them back through `IntrospectionRoot`, not by opening the
store table directly.
