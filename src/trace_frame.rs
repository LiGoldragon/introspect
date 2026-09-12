//! The component-trace wire: the contract's own rkyv Signal frame carried on a
//! `triad-runtime` trace socket.
//!
//! `signal-introspect` declares `ComponentTraceEvent` and forms it into a
//! `Signal<ComponentTraceEvent>`; `triad-runtime` owns the socket and asks a
//! trace event to say how it archives. This newtype is the one place the two
//! meet — the frame bytes are the contract's Signal bytes and nothing else, so
//! an emitting component and introspect agree on the wire by agreeing on the
//! contract.

use signal_introspect::{ByteViewable, ComponentTraceEvent, Restorable, Signal, Signalizable};
use triad_runtime::{TraceError, TraceEventFrame};

/// One component-trace event on its way over a trace socket.
#[derive(Debug, Clone, PartialEq)]
pub struct TracedComponentEvent(ComponentTraceEvent);

impl TracedComponentEvent {
    pub fn new(event: ComponentTraceEvent) -> Self {
        Self(event)
    }

    pub fn event(&self) -> &ComponentTraceEvent {
        &self.0
    }

    pub fn into_event(self) -> ComponentTraceEvent {
        self.0
    }
}

impl From<ComponentTraceEvent> for TracedComponentEvent {
    fn from(event: ComponentTraceEvent) -> Self {
        Self(event)
    }
}

impl TraceEventFrame for TracedComponentEvent {
    fn to_trace_archive(&self) -> Result<Vec<u8>, TraceError> {
        self.0
            .signalize()
            .map(|signal| signal.bytes().to_vec())
            .map_err(|_| TraceError::ArchiveEncode)
    }

    fn from_trace_archive(archive: &[u8]) -> Result<Self, TraceError> {
        Signal::<ComponentTraceEvent>::from(archive.to_vec())
            .restore()
            .map(Self)
            .map_err(|_| TraceError::ArchiveDecode)
    }
}
