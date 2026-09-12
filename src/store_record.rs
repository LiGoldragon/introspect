//! What introspect writes into `introspect.sema`, and the record keys it writes
//! them under.
//!
//! Each stored record wraps one contract value. The key is derived from the
//! value's own identifying positions, so a key-range scan over a query prefix
//! returns exactly the rows that query selects, in the order the query wants
//! them.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{EngineRecord, KeyRange, RecordKey};
use signal_introspect::{
    CoalescedSystemEvent, ComponentTraceEvent, ComponentTraceQuery, DeliveryTraceObservationEvent,
    DeliveryTraceObservationJoinKey, DeliveryTraceObservationKey, DeliveryTraceObservationQuery,
    IntrospectionTarget, Query, Response,
};

use crate::contract::CoalescedReading;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[rkyv(derive(Debug))]
pub struct ObservationSequence(u64);

impl ObservationSequence {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationReceipt {
    sequence: ObservationSequence,
    snapshot: sema_engine::SnapshotIdentifier,
}

impl ObservationReceipt {
    pub fn new(sequence: ObservationSequence, snapshot: sema_engine::SnapshotIdentifier) -> Self {
        Self { sequence, snapshot }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn snapshot(&self) -> sema_engine::SnapshotIdentifier {
        self.snapshot
    }
}

/// One answered query, kept with the response it produced.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct StoredObservation {
    sequence: ObservationSequence,
    query: Query,
    response: Response,
}

impl StoredObservation {
    pub fn new(sequence: ObservationSequence, query: Query, response: Response) -> Self {
        Self {
            sequence,
            query,
            response,
        }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn response(&self) -> &Response {
        &self.response
    }
}

impl EngineRecord for StoredObservation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.sequence.value().to_string())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct StoredDeliveryTraceEvent {
    event: DeliveryTraceObservationEvent,
}

impl StoredDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceObservationEvent) -> Self {
        Self { event }
    }

    pub fn event(&self) -> &DeliveryTraceObservationEvent {
        &self.event
    }
}

impl EngineRecord for StoredDeliveryTraceEvent {
    fn record_key(&self) -> RecordKey {
        let key = &self.event.delivery_trace_observation_key;
        RecordKey::new(format!(
            "{}/{:010}",
            delivery_trace_prefix(&join_key_of_event(key)),
            key.hop_index
        ))
    }
}

/// The join key of one recorded hop: the message the hop belongs to, without
/// the hop's own index.
pub fn join_key_of_event(key: &DeliveryTraceObservationKey) -> DeliveryTraceObservationJoinKey {
    DeliveryTraceObservationJoinKey {
        engine_identifier: key.engine_identifier.clone(),
        message_slot: key.message_slot,
        component_name: key.component_name.clone(),
    }
}

/// The join key a delivery-trace query selects.
pub fn join_key_of_query(query: &DeliveryTraceObservationQuery) -> DeliveryTraceObservationJoinKey {
    DeliveryTraceObservationJoinKey {
        engine_identifier: query.engine_identifier.clone(),
        message_slot: query.message_slot,
        component_name: query.component_name.clone(),
    }
}

/// The shared `engine/slot/originator` key prefix both a stored hop and a query
/// range derive from, so a stored hop and a query that selects it always agree
/// on the scan prefix.
pub fn delivery_trace_prefix(key: &DeliveryTraceObservationJoinKey) -> String {
    format!(
        "{}/{}/{}",
        key.engine_identifier, key.message_slot, key.component_name
    )
}

/// Key-range bounds for a delivery-trace query: every hop of the one message.
pub fn delivery_trace_range(query: &DeliveryTraceObservationQuery) -> KeyRange {
    let prefix = delivery_trace_prefix(&join_key_of_query(query));
    KeyRange::between(
        RecordKey::new(format!("{prefix}/")),
        RecordKey::new(format!("{prefix}/~")),
    )
}

/// Whether one recorded hop belongs to the message a query names.
pub fn delivery_trace_matches(
    event: &DeliveryTraceObservationEvent,
    query: &DeliveryTraceObservationQuery,
) -> bool {
    join_key_of_event(&event.delivery_trace_observation_key) == join_key_of_query(query)
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct StoredComponentTraceEvent {
    event: ComponentTraceEvent,
}

impl StoredComponentTraceEvent {
    pub fn new(event: ComponentTraceEvent) -> Self {
        Self { event }
    }

    pub fn event(&self) -> &ComponentTraceEvent {
        &self.event
    }
}

impl EngineRecord for StoredComponentTraceEvent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}/{:020}",
            component_trace_prefix(
                &self.event.engine_identifier,
                &self.event.introspection_target
            ),
            self.event.trace_sequence
        ))
    }
}

/// The shared `engine/component` key prefix a stored component-trace event and
/// a component-trace query range both derive from. The key sorts events by
/// component, then by zero-padded sequence, so a key-range scan over one
/// component returns its events in monotonic emission order.
pub fn component_trace_prefix(engine: &str, component: &IntrospectionTarget) -> String {
    format!("{engine}/{component:?}")
}

/// Key-range bounds for a component-trace query: every event whose key shares
/// the `engine/component` prefix. The event-name narrowing is an in-memory
/// filter applied after the scan, mirroring the delivery-trace range.
pub fn component_trace_range(query: &ComponentTraceQuery) -> KeyRange {
    let prefix = component_trace_prefix(&query.engine_identifier, &query.introspection_target);
    KeyRange::between(
        RecordKey::new(format!("{prefix}/")),
        RecordKey::new(format!("{prefix}/~")),
    )
}

/// Whether one recorded component-trace event satisfies a query, including its
/// optional event-name narrowing.
pub fn component_trace_matches(event: &ComponentTraceEvent, query: &ComponentTraceQuery) -> bool {
    event.engine_identifier == query.engine_identifier
        && event.introspection_target == query.introspection_target
        && query
            .optional_trace_event_name
            .as_ref()
            .is_none_or(|name| &event.trace_event_name == name)
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct StoredSystemEventSummary {
    summary: CoalescedSystemEvent,
}

impl StoredSystemEventSummary {
    pub fn new(summary: CoalescedSystemEvent) -> Self {
        Self { summary }
    }

    pub fn summary(&self) -> &CoalescedSystemEvent {
        &self.summary
    }
}

impl EngineRecord for StoredSystemEventSummary {
    fn record_key(&self) -> RecordKey {
        let event = self.summary.representative();
        RecordKey::new(format!(
            "{:016x}{:016x}/{:020}",
            event.boot_identifier.first_integer,
            event.boot_identifier.second_integer,
            event.event_identifier
        ))
    }
}
