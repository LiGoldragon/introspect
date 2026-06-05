# INTENT — introspect

`introspect` is the prototype's inspection-plane component: a supervised daemon and thin
CLI that let the engine explain itself through typed component observations. Its purpose
is a witness, not a broad UI. The first goal is concrete: after a fixture is delivered,
`introspect` asks the running components for typed observations and prints one NOTA proof
of what happened. It proves the delivery path after the fact; it is never in the delivery
path itself.

`introspect` fans out to peer component daemons over Signal and fans in their typed
observations as pushed subscription deltas. Each peer relationship is owned by one client
actor (`RouterClient`, `ManagerClient`, `TerminalClient`) that holds that peer's socket
path and speaks only that peer's observation contract. `RouterClient` is the first live
client; the others are honest scaffolds until their peer contracts and daemon ingress
land. The daemon owns its own typed `introspect.redb` through `sema-engine`, persisting
the query/reply/error audit trail, subscription registrations, and a delivery-trace cache
keyed by the introspection-owned `DeliveryTraceKey`.

Key constraints: every live observation crosses a component daemon boundary — peer state
is reached only through peer daemon sockets and component contracts, **never by opening
another component's redb file**. The daemon consumes `introspect.redb` exclusively through
`sema-engine`; there are no direct `redb` or `sema::open_with_schema` calls in this repo.
NOTA renders only at the human/agent edge — the CLI and projection surface — never on the
inter-component wire, where typed Signal replies travel. Peer observation is push
subscription when the peer stream exists; a one-shot Match query is allowed only as an
explicit prototype witness path, never as a timer loop — consumers do not poll.
`DeliveryTraceKey` is introspection-domain state (engine, message identifier, originator,
hop index), distinct from Signal exchange identity and request/reply correlation. Component
observations stay component-owned: `signal-introspect` wraps and correlates; per-component
observation vocabulary lives in each peer's own contract, so `signal-introspect` never
becomes a shared schema bucket.
