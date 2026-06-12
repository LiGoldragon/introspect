# introspect

Persona inspection-plane daemon with working and meta CLIs.

The prototype goal is not a broad UI. The first goal is a witness:
after fixture delivery, `introspect` should ask the running components for
typed observations and print one NOTA proof of what happened.

Entrypoints:

- `introspect-daemon` takes exactly one signal-encoded rkyv startup file.
- `introspect` takes one NOTA request or NOTA file and sends it to the
  working socket.
- `meta-introspect` takes one NOTA meta operation or NOTA file and sends it
  to the owner meta socket.
