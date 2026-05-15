# skills - persona-introspect

*Per-repo agent guide.*

## Checkpoint - read before editing

Before changing code in this repo, read:

- `~/primary/skills/operator.md`
- `~/primary/skills/kameo.md`
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/rust-discipline.md`
- `~/primary/skills/architectural-truth-tests.md`
- `~/primary/skills/nix-discipline.md`
- this repo's `ARCHITECTURE.md`
- `signal-persona-introspect/ARCHITECTURE.md`

## What this repo owns

- `persona-introspect-daemon`
- `introspect` CLI
- Runtime fan-out to component daemons over Signal.
- Local observation audit state in `introspect.redb`, opened through
  `sema-engine`.
- NOTA projection at the CLI edge.

## What this repo does not own

- Other components' redb files.
- Router, terminal, manager, harness, message, system, or mind policy.
- Component observation record definitions.

Live introspection asks component daemons. It never opens their databases.
