//! Consumer-side projections over the `signal-introspect` ethos root.
//!
//! The contract crate is generated from `ethos/signal.ethos`: it declares the
//! types and nothing else — no hand-written methods, no admission policy, no
//! derived identity. Everything below is introspect's own reading of those
//! types, and lives here rather than in the contract because it is the
//! component's behaviour, not the wire.
//!
//! The positional field names the generator emits for a struct whose ethos
//! anatomy repeats a type (`CoalescedSystemEvent`, `ExactCoalescingStatus`,
//! `SystemEventAccepted`) are ordinal; the names this module gives them are
//! what those positions mean to introspect.

use signal_introspect::{
    BluetoothPowerObservation, BluetoothSystemEvent, BluetoothTopic, CoalescedSystemEvent,
    CoalescingClosure, EventProvenance, EventSource, JournalSource, ProvenanceTrust,
    ServiceLifecycleObservation, SystemEvent, SystemEventDomain, SystemdSystemEvent, SystemdTopic,
    TargetedSystemEvent,
};
use thiserror::Error;

/// Which observation domain a targeted system event belongs to.
pub const fn system_event_domain(event: &TargetedSystemEvent) -> SystemEventDomain {
    match event {
        TargetedSystemEvent::Bluetooth(_) => SystemEventDomain::Hardware,
        TargetedSystemEvent::Systemd(_) => SystemEventDomain::ServiceControl,
    }
}

/// An unclassified observation carries counters and status only. It is the one
/// shape that must never arrive with payload text attached.
pub const fn is_unclassified(event: &TargetedSystemEvent) -> bool {
    matches!(
        event,
        TargetedSystemEvent::Bluetooth(BluetoothSystemEvent {
            bluetooth_topic: BluetoothTopic::Power(BluetoothPowerObservation::Unclassified(_)),
            ..
        }) | TargetedSystemEvent::Systemd(SystemdSystemEvent {
            systemd_topic: SystemdTopic::Lifecycle(ServiceLifecycleObservation::Unclassified(_)),
            ..
        })
    )
}

/// Provenance is consistent when the trust classification is the one its source
/// can actually support: journal metadata is trusted journal metadata, a bus
/// connection is trusted connection metadata, and an application's own claim is
/// untrusted.
pub const fn provenance_is_consistent(provenance: &EventProvenance) -> bool {
    matches!(
        (&provenance.event_source, &provenance.provenance_trust),
        (
            EventSource::Journal(_),
            ProvenanceTrust::TrustedJournalMetadata
        ) | (
            EventSource::BusConnection,
            ProvenanceTrust::TrustedConnectionMetadata
        ) | (
            EventSource::Application(_),
            ProvenanceTrust::UntrustedApplicationMetadata
        )
    )
}

/// Journal-sourced provenance at its only coherent trust level.
pub const fn trusted_journal(source: JournalSource) -> EventProvenance {
    EventProvenance {
        event_source: EventSource::Journal(source),
        provenance_trust: ProvenanceTrust::TrustedJournalMetadata,
    }
}

/// Why introspect refuses to record a targeted system event. Admission is the
/// component's policy: the contract carries the shape, introspect decides what
/// it will persist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SystemEventFault {
    #[error("event source and trust classification are inconsistent")]
    InconsistentProvenance,
    #[error("targeted unclassified events may retain counters/status only, never payload text")]
    UnclassifiedPayload,
}

/// Admit a targeted system event into durable state, or say why not.
pub fn validate_system_event(event: &SystemEvent) -> Result<(), SystemEventFault> {
    if !provenance_is_consistent(&event.event_provenance) {
        return Err(SystemEventFault::InconsistentProvenance);
    }
    if is_unclassified(&event.targeted_system_event) && event.bounded_payload_option.is_some() {
        return Err(SystemEventFault::UnclassifiedPayload);
    }
    Ok(())
}

/// The exact-duplicate identity of a system event: everything the event says
/// about *what happened*, with the identifier and the instant — everything that
/// says *which occurrence* — deliberately excluded.
///
/// The identity is the canonical datom text of the event with those two
/// positions normalised away. Datom text is the schema-driven projection of the
/// value, so two events share an identity exactly when every remaining position
/// holds an equal value; that makes the text a sound hash key for contract
/// types that carry neither `Hash` nor `Eq`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExactDuplicateIdentity(String);

impl ExactDuplicateIdentity {
    pub fn of(event: &SystemEvent) -> Self {
        let mut occurrence_free = event.clone();
        occurrence_free.event_identifier = 0;
        occurrence_free.event_instant = 0;
        Self(crate::datom_text::textualize(&occurrence_free))
    }
}

/// Open a fresh coalescing window around its first — and so far only — event.
pub fn coalesced(representative: SystemEvent, closure: CoalescingClosure) -> CoalescedSystemEvent {
    let observed_at = representative.event_instant;
    let policy_revision = representative.policy_revision;
    CoalescedSystemEvent {
        system_event: representative,
        first_integer: 1,
        first_event_instant: observed_at,
        second_event_instant: observed_at,
        second_integer: 0,
        policy_revision,
        coalescing_closure: closure,
    }
}

/// The positions of a `CoalescedSystemEvent`, read by what they mean.
pub trait CoalescedReading {
    /// The event that stands for the whole window.
    fn representative(&self) -> &SystemEvent;
    /// How many events the window has seen, the representative included.
    fn count(&self) -> i64;
    /// How many of those were suppressed as exact duplicates.
    fn suppressed_count(&self) -> i64;
    /// When the window opened.
    fn first_seen(&self) -> i64;
    /// When the window last saw an event.
    fn last_seen(&self) -> i64;
}

impl CoalescedReading for CoalescedSystemEvent {
    fn representative(&self) -> &SystemEvent {
        &self.system_event
    }

    fn count(&self) -> i64 {
        self.first_integer
    }

    fn suppressed_count(&self) -> i64 {
        self.second_integer
    }

    fn first_seen(&self) -> i64 {
        self.first_event_instant
    }

    fn last_seen(&self) -> i64 {
        self.second_event_instant
    }
}
