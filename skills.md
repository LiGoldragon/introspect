# skills - introspect

*Per-repo agent guide.*

## Checkpoint - read before editing

Before changing code in this repo, read:

- `~/primary/skills/operator.md`
- `~/primary/skills/kameo.md`
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/rust-discipline.md`
- `~/primary/skills/nix-discipline.md`
- `~/primary/skills/subscription-lifecycle.md`
- `~/primary/skills/push-not-pull.md`
- the `ethos` and `datom` skills
- this repo's `ARCHITECTURE.md`
- `signal-introspect/ARCHITECTURE.md`

## What this repo owns

- `introspect-daemon`
- `introspect` CLI
- `meta-introspect` CLI
- The uniform daemon shell in `src/daemon_shell.rs` — argv, binding, the two
  listener tiers, the connection spine, the exit report. It is component-
  agnostic; everything component-specific reaches it through `ComponentDaemon`.
- Runtime fan-out to component daemons over Signal.
- Local observation audit state in `introspect.sema`, opened through
  `sema-engine`.
- `introspect-daemon` starts from one signal-encoded rkyv
  `IntrospectDaemonConfiguration` file. Inline text and text files are
  rejected as startup arguments.
- The datom text plane at the CLI edge (`src/datom_text.rs`).
- Consumer-side readings of the contract types (`src/contract.rs`): the
  admission policy for a targeted system event, its exact-duplicate identity,
  and the ordinal positions of `CoalescedSystemEvent` read by what they mean.
- The ordinary CLI uses `INTROSPECT_SOCKET` and the meta CLI uses
  `INTROSPECT_META_SOCKET`; both are thin one-argument clients over the
  daemon sockets.

## What this repo does not own

- Other components' database files.
- Router, terminal, manager, harness, message, system, or mind policy.
- Component observation record definitions — those live in each peer's
  `ethos/signal.ethos` and reach this repo as generated Rust.

Live introspection asks component daemons. It never opens their databases.

## Contract discipline

Every contract type introspect speaks is generated from an ethos root and
carries `rkyv::Archive` plus, under the contracts' `datom` feature,
`datom_codec::Datomizable` and `datom_codec::Compositional`. Consequences:

- Never hand-write a `Datomic` impl, a text codec, or a reply projection. The
  type is the codec: `datom_text::actualize` reads, `datom_text::textualize`
  writes.
- The wire is `Signal<Query>` / `Signal<Response>` inside one triad
  length-prefixed body. There is no exchange envelope and no sub-reply nesting
  on introspect's own planes.
- Generated structs whose ethos anatomy repeats a type get ordinal field names
  (`first_integer`, `second_event_instant`). Read them through
  `contract::CoalescedReading` rather than spelling the ordinal at each use.
- Contract types derive `Clone, Debug, PartialEq` and nothing more — no `Eq`,
  no `Hash`, no `Copy`. A map key derived from a contract value is its
  canonical datom text, not a derived hash.
- Every dependency is pinned by `rev`, never by `branch`. A branch pin is what
  broke this estate.

`signal-router` is the one exception to the Signal-frame rule: it is a peer
contract still on the `signal-frame` exchange envelope, and `RouterClient`
speaks the peer's contract as the peer publishes it.

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
it sends one typed frame, parses the typed reply, and composes the result into
the carrier record (`PrototypeWitnessObservation`,
`ComponentSnapshotObservation`, etc.) — using `Some(state)` when the peer
answered, `None` when the peer socket is not configured or the peer daemon has
not yet shipped that contract operation.

Delivery-trace observations are keyed by
`signal_introspect::DeliveryTraceObservationKey`. The first three positions
(`engine_identifier`, `message_slot`, `component_name`) join one delivery
chain; `hop_index` orders the events. Tests should insert events out of order
and query them back through `IntrospectionRoot`, not by opening the
store table directly.
