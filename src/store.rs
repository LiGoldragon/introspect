use std::path::{Path, PathBuf};

use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible};
use kameo::message::{Context, Message};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitLogEntry, Engine, EngineOpen, EngineRecord, FamilyName, KeyRange, Mutation,
    QueryPlan, RecordKey, Retraction, SchemaHash, SchemaVersion, SnapshotIdentifier,
    TableDescriptor, TableName, TableReference, VersionedStoreName, VersioningPolicy,
};
use signal_introspect::{
    BootIdentifier, CoalescedSystemEvent, CoalescingClosure, ComponentTrace, ComponentTraceEvent,
    ComponentTraceQuery, DeliveryTrace, DeliveryTraceEvent, DeliveryTraceJoinKey,
    DeliveryTraceQuery, ExactCoalescingStatus, IntrospectionReply, IntrospectionRequest,
    IntrospectionTarget, SystemEvent, SystemEventAccepted, SystemEventSummaries, SystemEvents,
    SystemEventsFlushed, SystemEventsQuery,
};
use signal_persona::EngineIdentifier;

use crate::Result;
use crate::coalescer::{ExactCoalescingPolicy, ExactDuplicateCoalescer};

const INTROSPECTION_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const OBSERVATIONS: TableName = TableName::new("introspection_observations");
const DELIVERY_TRACE_EVENTS: TableName = TableName::new("delivery_trace_events");
const COMPONENT_TRACE_EVENTS: TableName = TableName::new("component_trace_events");
const SYSTEM_EVENT_SUMMARIES: TableName = TableName::new("system_event_summaries");
const OBSERVATIONS_FAMILY: &str = "introspection-observation";
const DELIVERY_TRACE_EVENTS_FAMILY: &str = "delivery-trace-event";
const COMPONENT_TRACE_EVENTS_FAMILY: &str = "component-trace-event";
const SYSTEM_EVENT_SUMMARIES_FAMILY: &str = "system-event-summary";

/// Per-table caps for diagnostic records kept in `introspect.sema`. The typed
/// policy is local component state until the meta configuration contract gains
/// a retention operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistenceRetention {
    observations: usize,
    delivery_trace_events: usize,
    component_trace_events: usize,
    system_event_summaries: usize,
}

impl PersistenceRetention {
    pub const fn new(
        observations: usize,
        delivery_trace_events: usize,
        component_trace_events: usize,
    ) -> Self {
        Self::with_system_event_summaries(
            observations,
            delivery_trace_events,
            component_trace_events,
            component_trace_events,
        )
    }

    pub const fn with_system_event_summaries(
        observations: usize,
        delivery_trace_events: usize,
        component_trace_events: usize,
        system_event_summaries: usize,
    ) -> Self {
        Self {
            observations,
            delivery_trace_events,
            component_trace_events,
            system_event_summaries,
        }
    }
}

impl Default for PersistenceRetention {
    fn default() -> Self {
        Self::new(1024, 4096, 4096)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    path: PathBuf,
}

impl StoreLocation {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Local tooling convenience: callers outside the daemon startup path may
    /// discover an ad hoc introspection store via `PERSONA_INTROSPECT_STORE` or
    /// `PERSONA_STATE_PATH`. The daemon opens only the store path supplied by
    /// `IntrospectDaemonConfiguration.store_path`.
    pub fn from_environment() -> Self {
        match std::env::var_os("PERSONA_INTROSPECT_STORE") {
            Some(path) => Self::new(path),
            None => match std::env::var_os("PERSONA_STATE_PATH") {
                Some(path) => Self::new(path),
                None => Self::new("/tmp/introspect.sema"),
            },
        }
    }

    pub fn as_path(&self) -> &Path {
        self.path.as_path()
    }
}

pub struct IntrospectionStore {
    engine: Engine,
    observations: TableReference<StoredObservation>,
    delivery_trace_events: TableReference<StoredDeliveryTraceEvent>,
    component_trace_events: TableReference<StoredComponentTraceEvent>,
    system_event_summaries: TableReference<StoredSystemEventSummary>,
    retention: PersistenceRetention,
    exact_coalescer: ExactDuplicateCoalescer,
}

impl IntrospectionStore {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        Self::open_with_retention(store, PersistenceRetention::default())
    }

    pub fn open_with_retention(
        store: &StoreLocation,
        retention: PersistenceRetention,
    ) -> Result<Self> {
        Self::open_with_policies(store, retention, ExactCoalescingPolicy::default())
    }

    pub fn open_with_policies(
        store: &StoreLocation,
        retention: PersistenceRetention,
        exact_coalescing: ExactCoalescingPolicy,
    ) -> Result<Self> {
        let mut engine = Engine::open(Self::engine_open(store.as_path()))?;
        let observations =
            engine.register_table(Self::family_descriptor(OBSERVATIONS, OBSERVATIONS_FAMILY))?;
        let delivery_trace_events = engine.register_table(Self::family_descriptor(
            DELIVERY_TRACE_EVENTS,
            DELIVERY_TRACE_EVENTS_FAMILY,
        ))?;
        let component_trace_events = engine.register_table(Self::family_descriptor(
            COMPONENT_TRACE_EVENTS,
            COMPONENT_TRACE_EVENTS_FAMILY,
        ))?;
        let system_event_summaries = engine.register_table(Self::system_event_descriptor())?;
        let store = Self {
            engine,
            observations,
            delivery_trace_events,
            component_trace_events,
            system_event_summaries,
            retention,
            exact_coalescer: ExactDuplicateCoalescer::new(exact_coalescing),
        };
        store.reclaim_observations()?;
        store.reclaim_delivery_trace_events()?;
        store.reclaim_component_trace_events()?;
        store.reclaim_system_event_summaries()?;
        Ok(store)
    }

    fn engine_open(path: &Path) -> EngineOpen {
        EngineOpen::new(path.to_path_buf(), INTROSPECTION_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("introspect"))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!(
                "introspect-{family}-v{}",
                INTROSPECTION_SCHEMA_VERSION.value()
            )),
        )
    }

    /// The system-event family starts at archive version 1. Registration is an
    /// additive migration under the existing sema kernel schema version: no
    /// existing table archive changes, so version-3 stores remain readable.
    fn system_event_descriptor() -> TableDescriptor<StoredSystemEventSummary> {
        TableDescriptor::new(
            SYSTEM_EVENT_SUMMARIES,
            FamilyName::new(SYSTEM_EVENT_SUMMARIES_FAMILY),
            SchemaHash::for_label("introspect-system-event-summary-v1"),
        )
    }

    pub fn record_observation(&self, observation: StoredObservation) -> Result<ObservationReceipt> {
        let receipt = self
            .engine
            .assert(Assertion::new(self.observations, observation.clone()))?;
        self.reclaim_observations()?;
        Ok(ObservationReceipt::new(
            observation.sequence(),
            receipt.snapshot(),
        ))
    }

    pub fn observations(&self) -> Result<Vec<StoredObservation>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.observations))?
            .records()
            .to_vec())
    }

    pub fn record_delivery_trace_event(
        &self,
        event: DeliveryTraceEvent,
    ) -> Result<ObservationReceipt> {
        let stored_event = StoredDeliveryTraceEvent::new(event.clone());
        let receipt = self
            .engine
            .assert(Assertion::new(self.delivery_trace_events, stored_event))?;
        self.reclaim_delivery_trace_events()?;
        Ok(ObservationReceipt::new(
            ObservationSequence::new(event.key().hop_index.value() as u64),
            receipt.snapshot(),
        ))
    }

    pub fn delivery_trace(&self, query: DeliveryTraceQuery) -> Result<DeliveryTrace> {
        let mut events = self
            .engine
            .match_records(QueryPlan::key_range(
                self.delivery_trace_events,
                DeliveryTraceQueryRange::from_query(&query).into_range(),
            ))?
            .records()
            .iter()
            .filter(|stored_event| stored_event.event().key().matches_query(&query))
            .map(|stored_event| stored_event.event().clone())
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.key().hop_index);
        Ok(DeliveryTrace::new(
            query.engine,
            query.message_identifier,
            query.originator,
            events,
        ))
    }

    pub fn record_component_trace_event(
        &self,
        event: ComponentTraceEvent,
    ) -> Result<ObservationReceipt> {
        let sequence = event.sequence.value();
        let stored_event = StoredComponentTraceEvent::new(event);
        let receipt = self
            .engine
            .assert(Assertion::new(self.component_trace_events, stored_event))?;
        self.reclaim_component_trace_events()?;
        Ok(ObservationReceipt::new(
            ObservationSequence::new(sequence),
            receipt.snapshot(),
        ))
    }

    pub fn component_trace(&self, query: ComponentTraceQuery) -> Result<ComponentTrace> {
        let mut events = self
            .engine
            .match_records(QueryPlan::key_range(
                self.component_trace_events,
                ComponentTraceQueryRange::from_query(&query).into_range(),
            ))?
            .records()
            .iter()
            .filter(|stored_event| stored_event.event().matches_query(&query))
            .map(|stored_event| stored_event.event().clone())
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.sequence);
        Ok(ComponentTrace::new(query.engine, query.component, events))
    }

    fn reclaim_observations(&self) -> Result<()> {
        let mut records = self.observations()?;
        records.sort_by_key(|record| record.sequence().value());
        let excess = records.len().saturating_sub(self.retention.observations);
        for record in records.into_iter().take(excess) {
            self.engine
                .retract(Retraction::new(self.observations, record.record_key()))?;
        }
        Ok(())
    }

    fn reclaim_delivery_trace_events(&self) -> Result<()> {
        let records = self
            .engine
            .match_records(QueryPlan::all(self.delivery_trace_events))?
            .records()
            .to_vec();
        let excess = records
            .len()
            .saturating_sub(self.retention.delivery_trace_events);
        for record in records.into_iter().take(excess) {
            self.engine.retract(Retraction::new(
                self.delivery_trace_events,
                record.record_key(),
            ))?;
        }
        Ok(())
    }

    fn reclaim_component_trace_events(&self) -> Result<()> {
        let records = self
            .engine
            .match_records(QueryPlan::all(self.component_trace_events))?
            .records()
            .to_vec();
        let excess = records
            .len()
            .saturating_sub(self.retention.component_trace_events);
        for record in records.into_iter().take(excess) {
            self.engine.retract(Retraction::new(
                self.component_trace_events,
                record.record_key(),
            ))?;
        }
        Ok(())
    }

    fn reclaim_system_event_summaries(&self) -> Result<()> {
        let mut records = self
            .engine
            .match_records(QueryPlan::all(self.system_event_summaries))?
            .records()
            .to_vec();
        records.sort_by_key(|record| {
            (
                record.summary().first_seen,
                record.summary().representative.identifier,
            )
        });
        let excess = records
            .len()
            .saturating_sub(self.retention.system_event_summaries);
        for record in records.into_iter().take(excess) {
            self.engine.retract(Retraction::new(
                self.system_event_summaries,
                record.record_key(),
            ))?;
        }
        Ok(())
    }

    pub fn record_system_event(&mut self, event: SystemEvent) -> Result<SystemEventAccepted> {
        event.validate().map_err(crate::Error::from)?;
        let update = self.exact_coalescer.ingest(event);
        for summary in update.closed() {
            self.persist_system_event_summary(summary.clone())?;
        }
        self.persist_system_event_summary(update.current().clone())?;
        Ok(update.receipt())
    }

    pub fn system_events(&self, query: SystemEventsQuery) -> Result<SystemEvents> {
        let mut summaries = self
            .engine
            .match_records(QueryPlan::all(self.system_event_summaries))?
            .records()
            .iter()
            .map(|stored| stored.summary().clone())
            .filter(|summary| summary.representative.boot == query.boot)
            .filter(|summary| {
                query
                    .domain
                    .is_none_or(|domain| summary.representative.classification.domain() == domain)
            })
            .collect::<Vec<_>>();
        summaries.sort_by_key(|summary| (summary.first_seen, summary.representative.identifier));
        Ok(SystemEvents {
            boot: query.boot,
            summaries: SystemEventSummaries::new(summaries),
            coalescing: self.exact_coalescer.status(),
        })
    }

    pub fn flush_system_events(
        &mut self,
        boot: BootIdentifier,
        closure: CoalescingClosure,
    ) -> Result<SystemEventsFlushed> {
        let summaries = self.exact_coalescer.flush_boot(&boot, closure);
        for summary in &summaries {
            self.persist_system_event_summary(summary.clone())?;
        }
        Ok(SystemEventsFlushed {
            boot,
            summaries: SystemEventSummaries::new(summaries),
        })
    }

    pub fn exact_coalescing_status(&self) -> ExactCoalescingStatus {
        self.exact_coalescer.status()
    }

    fn persist_system_event_summary(&self, summary: CoalescedSystemEvent) -> Result<()> {
        let stored = StoredSystemEventSummary::new(summary);
        let key = stored.record_key();
        let exists = self
            .engine
            .match_records(QueryPlan::all(self.system_event_summaries))?
            .records()
            .iter()
            .any(|record| record.record_key() == key);
        if exists {
            self.engine
                .mutate(Mutation::new(self.system_event_summaries, stored))?;
        } else {
            self.engine
                .assert(Assertion::new(self.system_event_summaries, stored))?;
        }
        self.reclaim_system_event_summaries()?;
        Ok(())
    }

    pub fn operation_log(&self) -> Result<Vec<CommitLogEntry>> {
        Ok(self.engine.commit_log()?)
    }
}

impl Actor for IntrospectionStore {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> std::result::Result<(), Self::Error> {
        let summaries = self.exact_coalescer.flush_all(CoalescingClosure::Shutdown);
        for summary in summaries {
            let _ = self.persist_system_event_summary(summary);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordObservation {
    observation: StoredObservation,
}

impl RecordObservation {
    pub fn new(observation: StoredObservation) -> Self {
        Self { observation }
    }
}

impl Message<RecordObservation> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_observation(message.observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDeliveryTraceEvent {
    event: DeliveryTraceEvent,
}

impl RecordDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordDeliveryTraceEvent> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordDeliveryTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_delivery_trace_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDeliveryTrace {
    query: DeliveryTraceQuery,
}

impl ReadDeliveryTrace {
    pub fn new(query: DeliveryTraceQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadDeliveryTrace> for IntrospectionStore {
    type Reply = Result<DeliveryTrace>;

    async fn handle(
        &mut self,
        message: ReadDeliveryTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.delivery_trace(message.query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordComponentTraceEvent {
    event: ComponentTraceEvent,
}

impl RecordComponentTraceEvent {
    pub fn new(event: ComponentTraceEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordComponentTraceEvent> for IntrospectionStore {
    type Reply = Result<ObservationReceipt>;

    async fn handle(
        &mut self,
        message: RecordComponentTraceEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_component_trace_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadComponentTrace {
    query: ComponentTraceQuery,
}

impl ReadComponentTrace {
    pub fn new(query: ComponentTraceQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadComponentTrace> for IntrospectionStore {
    type Reply = Result<ComponentTrace>;

    async fn handle(
        &mut self,
        message: ReadComponentTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.component_trace(message.query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTargetedSystemEvent {
    event: SystemEvent,
}

impl RecordTargetedSystemEvent {
    pub fn new(event: SystemEvent) -> Self {
        Self { event }
    }
}

impl Message<RecordTargetedSystemEvent> for IntrospectionStore {
    type Reply = Result<SystemEventAccepted>;

    async fn handle(
        &mut self,
        message: RecordTargetedSystemEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_system_event(message.event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSystemEvents {
    query: SystemEventsQuery,
}

impl ReadSystemEvents {
    pub fn new(query: SystemEventsQuery) -> Self {
        Self { query }
    }
}

impl Message<ReadSystemEvents> for IntrospectionStore {
    type Reply = Result<SystemEvents>;

    async fn handle(
        &mut self,
        message: ReadSystemEvents,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.system_events(message.query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushTargetedSystemEvents {
    boot: BootIdentifier,
}

impl FlushTargetedSystemEvents {
    pub fn new(boot: BootIdentifier) -> Self {
        Self { boot }
    }
}

impl Message<FlushTargetedSystemEvents> for IntrospectionStore {
    type Reply = Result<SystemEventsFlushed>;

    async fn handle(
        &mut self,
        message: FlushTargetedSystemEvents,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.flush_system_events(message.boot, CoalescingClosure::ExplicitFlush)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadObservations;

impl Message<ReadObservations> for IntrospectionStore {
    type Reply = Result<Vec<StoredObservation>>;

    async fn handle(
        &mut self,
        _message: ReadObservations,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observations()
    }
}

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
    snapshot: SnapshotIdentifier,
}

impl ObservationReceipt {
    pub fn new(sequence: ObservationSequence, snapshot: SnapshotIdentifier) -> Self {
        Self { sequence, snapshot }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn snapshot(&self) -> SnapshotIdentifier {
        self.snapshot
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredObservation {
    sequence: ObservationSequence,
    request: IntrospectionRequest,
    reply: IntrospectionReply,
}

impl StoredObservation {
    pub fn new(
        sequence: ObservationSequence,
        request: IntrospectionRequest,
        reply: IntrospectionReply,
    ) -> Self {
        Self {
            sequence,
            request,
            reply,
        }
    }

    pub fn sequence(&self) -> ObservationSequence {
        self.sequence
    }

    pub fn request(&self) -> &IntrospectionRequest {
        &self.request
    }

    pub fn reply(&self) -> &IntrospectionReply {
        &self.reply
    }
}

impl EngineRecord for StoredObservation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.sequence.value().to_string())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDeliveryTraceEvent {
    event: DeliveryTraceEvent,
}

impl StoredDeliveryTraceEvent {
    pub fn new(event: DeliveryTraceEvent) -> Self {
        Self { event }
    }

    pub fn event(&self) -> &DeliveryTraceEvent {
        &self.event
    }
}

impl EngineRecord for StoredDeliveryTraceEvent {
    fn record_key(&self) -> RecordKey {
        DeliveryTraceEventRecordKey::from_event(&self.event).into_record_key()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryTraceEventRecordKey {
    event: DeliveryTraceEvent,
}

impl DeliveryTraceEventRecordKey {
    fn from_event(event: &DeliveryTraceEvent) -> Self {
        Self {
            event: event.clone(),
        }
    }

    fn into_record_key(self) -> RecordKey {
        let key = self.event.key();
        let join_key = DeliveryTraceJoinKeyPrefix::from_join_key(&key.join_key()).into_string();
        RecordKey::new(format!("{}/{:010}", join_key, key.hop_index.value()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryTraceQueryRange {
    query: DeliveryTraceQuery,
}

impl DeliveryTraceQueryRange {
    fn from_query(query: &DeliveryTraceQuery) -> Self {
        Self {
            query: query.clone(),
        }
    }

    fn into_range(self) -> KeyRange {
        let prefix =
            DeliveryTraceJoinKeyPrefix::from_join_key(&self.query.join_key()).into_string();
        KeyRange::between(
            RecordKey::new(format!("{prefix}/")),
            RecordKey::new(format!("{prefix}/~")),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryTraceJoinKeyPrefix {
    key: DeliveryTraceJoinKey,
}

impl DeliveryTraceJoinKeyPrefix {
    fn from_join_key(key: &DeliveryTraceJoinKey) -> Self {
        Self { key: key.clone() }
    }

    fn into_string(self) -> String {
        format!(
            "{}/{}/{}",
            self.key.engine.payload().as_str(),
            self.key.message_identifier.clone().into_u64(),
            self.key.originator.payload().as_str()
        )
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
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
        ComponentTraceEventRecordKey::from_event(&self.event).into_record_key()
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
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
        let event = &self.summary.representative;
        RecordKey::new(format!(
            "{:016x}{:016x}/{:020}",
            event.boot.high(),
            event.boot.low(),
            event.identifier.value()
        ))
    }
}

/// Per-event record key for `component_trace_events`: the component-trace
/// equivalent of `DeliveryTraceEventRecordKey`. The key sorts events by
/// `engine/component`, then by zero-padded `sequence` so a key-range scan
/// over one component returns its events in monotonic emission order.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ComponentTraceEventRecordKey {
    event: ComponentTraceEvent,
}

impl ComponentTraceEventRecordKey {
    fn from_event(event: &ComponentTraceEvent) -> Self {
        Self {
            event: event.clone(),
        }
    }

    fn into_record_key(self) -> RecordKey {
        let prefix =
            ComponentTraceKeyPrefix::new(&self.event.engine, self.event.component).into_string();
        RecordKey::new(format!("{}/{:020}", prefix, self.event.sequence.value()))
    }
}

/// Key-range bounds for a `ComponentTraceQuery`: every event whose key shares
/// the `engine/component` prefix, regardless of sequence or event name (the
/// `event_name` narrowing is applied as an in-memory filter after the scan,
/// mirroring `DeliveryTraceQueryRange`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ComponentTraceQueryRange {
    query: ComponentTraceQuery,
}

impl ComponentTraceQueryRange {
    fn from_query(query: &ComponentTraceQuery) -> Self {
        Self {
            query: query.clone(),
        }
    }

    fn into_range(self) -> KeyRange {
        let prefix =
            ComponentTraceKeyPrefix::new(&self.query.engine, self.query.component).into_string();
        KeyRange::between(
            RecordKey::new(format!("{prefix}/")),
            RecordKey::new(format!("{prefix}/~")),
        )
    }
}

/// The shared `engine/component` key prefix both the per-event record key and
/// the query range derive from, so a stored event and a query that selects it
/// always agree on the scan prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ComponentTraceKeyPrefix {
    engine: EngineIdentifier,
    component: IntrospectionTarget,
}

impl ComponentTraceKeyPrefix {
    fn new(engine: &EngineIdentifier, component: IntrospectionTarget) -> Self {
        Self {
            engine: engine.clone(),
            component,
        }
    }

    fn into_string(self) -> String {
        format!("{}/{:?}", self.engine.payload().as_str(), self.component)
    }
}
