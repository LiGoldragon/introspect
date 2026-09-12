# introspect

Persona inspection-plane daemon with working and meta CLIs.

The prototype goal is not a broad UI. The first goal is a witness:
after fixture delivery, `introspect` should ask the running components for
typed observations and print one datom proof of what happened.

The wire is the contract's own rkyv Signal frame; the text is datom. Both
come from `ethos/signal.ethos` in the contract repositories — one ethos root
generates the Rust, and that generated type is the whole interface. Introspect
writes no codec of its own.

Entrypoints:

- `introspect-daemon` takes exactly one signal-encoded rkyv startup file: the
  `IntrospectDaemonConfiguration` the Persona manager encodes.
- `introspect` takes one datom `signal_introspect::Query` — inline or in a
  file — sends it to the working socket, and prints the datom `Response`.
- `meta-introspect` takes one datom `meta_signal_introspect::Query` the same
  way and sends it to the owner meta socket.

```sh
introspect 'PrototypeWitnessObservation.{ prototype }'
```
