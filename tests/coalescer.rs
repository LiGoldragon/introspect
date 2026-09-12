//! Exact-duplicate coalescing: identity, window closure, eviction, and boot
//! partitioning.

use introspect::coalescer::{ExactCoalescingPolicy, ExactDuplicateCoalescer};
use introspect::contract::{CoalescedReading, trusted_journal};
use signal_introspect::{
    BluetoothPowerEvent, BluetoothPowerObservation, BluetoothSystemEvent, BluetoothTarget,
    BluetoothTopic, BootIdentifier, CoalescingClosure, EventSeverity, JournalSource, SystemEvent,
    TargetedSystemEvent,
};

struct CoalescerFixture;

impl CoalescerFixture {
    fn boot(first: i64, second: i64) -> BootIdentifier {
        BootIdentifier {
            first_integer: first,
            second_integer: second,
        }
    }

    fn event(identifier: i64, observed_at: i64, state: BluetoothPowerEvent) -> SystemEvent {
        Self::event_on_boot(identifier, Self::boot(1, 2), observed_at, state)
    }

    fn event_on_boot(
        identifier: i64,
        boot: BootIdentifier,
        observed_at: i64,
        state: BluetoothPowerEvent,
    ) -> SystemEvent {
        SystemEvent {
            event_identifier: identifier,
            boot_identifier: boot,
            event_instant: observed_at,
            targeted_system_event: TargetedSystemEvent::Bluetooth(BluetoothSystemEvent {
                bluetooth_target: BluetoothTarget::Controller,
                bluetooth_topic: BluetoothTopic::Power(BluetoothPowerObservation::Event(state)),
            }),
            event_severity: EventSeverity::Warning,
            event_provenance: trusted_journal(JournalSource::SystemdBluetoothService),
            extractor_revision: 1,
            policy_revision: 7,
            bounded_payload_option: None,
        }
    }
}

#[test]
fn exact_duplicates_preserve_representative_identity_and_seen_range() {
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10_000, 100));
    let first = coalescer.ingest(CoalescerFixture::event(
        10,
        1,
        BluetoothPowerEvent::ObservedOn,
    ));
    let second = coalescer.ingest(CoalescerFixture::event(
        11,
        9,
        BluetoothPowerEvent::ObservedOn,
    ));

    assert_eq!(first.current().count(), 1);
    assert_eq!(second.current().representative().event_identifier, 10);
    assert_eq!(second.current().count(), 2);
    assert_eq!(second.current().suppressed_count(), 1);
    assert_eq!(second.current().first_seen(), 1);
    assert_eq!(second.current().last_seen(), 9);
    assert_eq!(second.current().policy_revision, 7);
    assert_eq!(
        second.current().coalescing_closure,
        CoalescingClosure::Active
    );
}

#[test]
fn interval_closure_emits_summary_before_opening_the_next_window() {
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10_000, 10));
    coalescer.ingest(CoalescerFixture::event(
        1,
        1,
        BluetoothPowerEvent::ObservedOn,
    ));
    let update = coalescer.ingest(CoalescerFixture::event(
        2,
        11,
        BluetoothPowerEvent::ObservedOn,
    ));

    assert_eq!(update.closed().len(), 1);
    assert_eq!(
        update.closed()[0].coalescing_closure,
        CoalescingClosure::Interval
    );
    assert_eq!(update.current().representative().event_identifier, 2);
    assert_eq!(update.current().count(), 1);
}

#[test]
fn active_key_bound_evicts_oldest_with_observable_status() {
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(1, 100));
    coalescer.ingest(CoalescerFixture::event(
        1,
        1,
        BluetoothPowerEvent::ObservedOn,
    ));
    let update = coalescer.ingest(CoalescerFixture::event(
        2,
        2,
        BluetoothPowerEvent::ObservedOff,
    ));

    assert_eq!(update.closed().len(), 1);
    assert_eq!(
        update.closed()[0].coalescing_closure,
        CoalescingClosure::Eviction
    );
    assert_eq!(update.closed()[0].representative().event_identifier, 1);
    let status = coalescer.status();
    assert_eq!(status.first_integer, 1, "active keys");
    assert_eq!(status.second_integer, 1, "evictions");
    assert_eq!(status.third_integer, 1, "maximum active keys");
}

#[test]
fn interval_comparison_never_crosses_boot_partitions() {
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10, 10));
    coalescer.ingest(CoalescerFixture::event_on_boot(
        1,
        CoalescerFixture::boot(1, 1),
        100,
        BluetoothPowerEvent::ObservedOn,
    ));
    let other_boot = coalescer.ingest(CoalescerFixture::event_on_boot(
        2,
        CoalescerFixture::boot(2, 2),
        1,
        BluetoothPowerEvent::ObservedOn,
    ));

    assert!(other_boot.closed().is_empty());
    assert_eq!(coalescer.status().first_integer, 2);
}

#[test]
fn explicit_and_shutdown_flushes_are_distinct_closure_mechanisms() {
    let boot = CoalescerFixture::boot(1, 2);
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10, 100));
    coalescer.ingest(CoalescerFixture::event(
        1,
        1,
        BluetoothPowerEvent::ObservedOn,
    ));
    let explicit = coalescer.flush_boot(&boot, CoalescingClosure::ExplicitFlush);
    assert_eq!(explicit.len(), 1);
    assert_eq!(
        explicit[0].coalescing_closure,
        CoalescingClosure::ExplicitFlush
    );

    coalescer.ingest(CoalescerFixture::event(
        2,
        2,
        BluetoothPowerEvent::ObservedOff,
    ));
    let shutdown = coalescer.flush_all(CoalescingClosure::Shutdown);
    assert_eq!(shutdown.len(), 1);
    assert_eq!(shutdown[0].coalescing_closure, CoalescingClosure::Shutdown);
}
