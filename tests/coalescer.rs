use introspect::coalescer::{ExactCoalescingPolicy, ExactDuplicateCoalescer};
use signal_introspect::{
    BluetoothPowerEvent, BluetoothPowerObservation, BluetoothSystemEvent, BluetoothTarget,
    BluetoothTopic, BootIdentifier, CoalescingClosure, EventIdentifier, EventInstant,
    EventProvenance, EventSeverity, ExtractorRevision, JournalSource, PolicyRevision, SystemEvent,
    TargetedSystemEvent,
};

struct CoalescerFixture;

impl CoalescerFixture {
    fn event(identifier: u64, observed_at: u64, state: BluetoothPowerEvent) -> SystemEvent {
        Self::event_on_boot(identifier, BootIdentifier::new(1, 2), observed_at, state)
    }

    fn event_on_boot(
        identifier: u64,
        boot: BootIdentifier,
        observed_at: u64,
        state: BluetoothPowerEvent,
    ) -> SystemEvent {
        SystemEvent {
            identifier: EventIdentifier::new(identifier),
            boot,
            observed_at: EventInstant::new(observed_at),
            classification: TargetedSystemEvent::Bluetooth(BluetoothSystemEvent {
                target: BluetoothTarget::Controller,
                topic: BluetoothTopic::Power(BluetoothPowerObservation::Event(state)),
            }),
            severity: EventSeverity::Warning,
            provenance: EventProvenance::trusted_journal(JournalSource::SystemdBluetoothService),
            extractor_revision: ExtractorRevision::new(1),
            policy_revision: PolicyRevision::new(7),
            payload: None,
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

    assert_eq!(first.current().count, 1);
    assert_eq!(second.current().representative.identifier.value(), 10);
    assert_eq!(second.current().count, 2);
    assert_eq!(second.current().suppressed_count, 1);
    assert_eq!(second.current().first_seen.value(), 1);
    assert_eq!(second.current().last_seen.value(), 9);
    assert_eq!(second.current().policy_revision, PolicyRevision::new(7));
    assert_eq!(second.current().closure, CoalescingClosure::Active);
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
    assert_eq!(update.closed()[0].closure, CoalescingClosure::Interval);
    assert_eq!(update.current().representative.identifier.value(), 2);
    assert_eq!(update.current().count, 1);
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
    assert_eq!(update.closed()[0].closure, CoalescingClosure::Eviction);
    assert_eq!(update.closed()[0].representative.identifier.value(), 1);
    assert_eq!(coalescer.status().active_keys, 1);
    assert_eq!(coalescer.status().evictions, 1);
    assert_eq!(coalescer.status().maximum_active_keys, 1);
}

#[test]
fn interval_comparison_never_crosses_boot_partitions() {
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10, 10));
    coalescer.ingest(CoalescerFixture::event_on_boot(
        1,
        BootIdentifier::new(1, 1),
        100,
        BluetoothPowerEvent::ObservedOn,
    ));
    let other_boot = coalescer.ingest(CoalescerFixture::event_on_boot(
        2,
        BootIdentifier::new(2, 2),
        1,
        BluetoothPowerEvent::ObservedOn,
    ));

    assert!(other_boot.closed().is_empty());
    assert_eq!(coalescer.status().active_keys, 2);
}

#[test]
fn explicit_and_shutdown_flushes_are_distinct_closure_mechanisms() {
    let boot = BootIdentifier::new(1, 2);
    let mut coalescer = ExactDuplicateCoalescer::new(ExactCoalescingPolicy::new(10, 100));
    coalescer.ingest(CoalescerFixture::event(
        1,
        1,
        BluetoothPowerEvent::ObservedOn,
    ));
    let explicit = coalescer.flush_boot(&boot, CoalescingClosure::ExplicitFlush);
    assert_eq!(explicit.len(), 1);
    assert_eq!(explicit[0].closure, CoalescingClosure::ExplicitFlush);

    coalescer.ingest(CoalescerFixture::event(
        2,
        2,
        BluetoothPowerEvent::ObservedOff,
    ));
    let shutdown = coalescer.flush_all(CoalescingClosure::Shutdown);
    assert_eq!(shutdown.len(), 1);
    assert_eq!(shutdown[0].closure, CoalescingClosure::Shutdown);
}
