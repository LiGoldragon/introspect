//! Introspect's own durable state: a `sema-engine` store of answered
//! observations, delivery-trace hops, component-trace events and coalesced
//! system-event summaries. Introspect opens this store and no other — peer
//! observations cross daemon sockets, never peer database files.

use std::path::{Path, PathBuf};

use sema_engine::{
    Assertion, CommitLogEntry, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_introspect::{
    BootIdentifier, CoalescedSystemEvent, CoalescingClosure, ComponentTrace, ComponentTraceEvent,
    ComponentTraceQuery, DeliveryTraceObservation, DeliveryTraceObservationEvent,
    DeliveryTraceObservationQuery, ExactCoalescingStatus, SystemEvent, SystemEventAccepted,
    SystemEvents, SystemEventsFlushed, SystemEventsQuery,
};

use crate::Result;
use crate::coalescer::{ExactCoalescingPolicy, ExactDuplicateCoalescer};
use crate::contract::{CoalescedReading, system_event_domain, validate_system_event};
use crate::store_record::{
    ObservationReceipt, ObservationSequence, StoredComponentTraceEvent, StoredDeliveryTraceEvent,
    StoredObservation, StoredSystemEventSummary, component_trace_matches, component_trace_range,
    delivery_trace_matches, delivery_trace_range,
};

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
        event: DeliveryTraceObservationEvent,
    ) -> Result<ObservationReceipt> {
        let hop_index = event.delivery_trace_observation_key.hop_index;
        let stored_event = StoredDeliveryTraceEvent::new(event);
        let receipt = self
            .engine
            .assert(Assertion::new(self.delivery_trace_events, stored_event))?;
        self.reclaim_delivery_trace_events()?;
        Ok(ObservationReceipt::new(
            ObservationSequence::new(hop_index as u64),
            receipt.snapshot(),
        ))
    }

    pub fn delivery_trace(
        &self,
        query: DeliveryTraceObservationQuery,
    ) -> Result<DeliveryTraceObservation> {
        let mut events = self
            .engine
            .match_records(QueryPlan::key_range(
                self.delivery_trace_events,
                delivery_trace_range(&query),
            ))?
            .records()
            .iter()
            .filter(|stored| delivery_trace_matches(stored.event(), &query))
            .map(|stored| stored.event().clone())
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.delivery_trace_observation_key.hop_index);
        Ok(DeliveryTraceObservation {
            engine_identifier: query.engine_identifier,
            message_slot: query.message_slot,
            component_name: query.component_name,
            delivery_trace_observation_events: events,
        })
    }

    pub fn record_component_trace_event(
        &self,
        event: ComponentTraceEvent,
    ) -> Result<ObservationReceipt> {
        let sequence = event.trace_sequence;
        let stored_event = StoredComponentTraceEvent::new(event);
        let receipt = self
            .engine
            .assert(Assertion::new(self.component_trace_events, stored_event))?;
        self.reclaim_component_trace_events()?;
        Ok(ObservationReceipt::new(
            ObservationSequence::new(sequence as u64),
            receipt.snapshot(),
        ))
    }

    pub fn component_trace(&self, query: ComponentTraceQuery) -> Result<ComponentTrace> {
        let mut events = self
            .engine
            .match_records(QueryPlan::key_range(
                self.component_trace_events,
                component_trace_range(&query),
            ))?
            .records()
            .iter()
            .filter(|stored| component_trace_matches(stored.event(), &query))
            .map(|stored| stored.event().clone())
            .collect::<Vec<_>>();
        events.sort_by_key(|event| event.trace_sequence);
        Ok(ComponentTrace {
            engine_identifier: query.engine_identifier,
            introspection_target: query.introspection_target,
            component_trace_events: events,
        })
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
                record.summary().first_seen(),
                record.summary().representative().event_identifier,
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
        validate_system_event(&event).map_err(|fault| crate::Error::UnexpectedArgument {
            got: fault.to_string(),
        })?;
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
            .filter(|summary| summary.representative().boot_identifier == query.boot_identifier)
            .filter(|summary| {
                query
                    .system_event_domain_option
                    .as_ref()
                    .is_none_or(|domain| {
                        &system_event_domain(&summary.representative().targeted_system_event)
                            == domain
                    })
            })
            .collect::<Vec<_>>();
        summaries.sort_by_key(|summary| {
            (
                summary.first_seen(),
                summary.representative().event_identifier,
            )
        });
        Ok(SystemEvents {
            boot_identifier: query.boot_identifier,
            system_event_summaries: summaries,
            exact_coalescing_status: self.exact_coalescer.status(),
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
            boot_identifier: boot,
            system_event_summaries: summaries,
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

    pub(crate) fn flush_on_shutdown(&mut self) {
        let summaries = self.exact_coalescer.flush_all(CoalescingClosure::Shutdown);
        for summary in summaries {
            let _ = self.persist_system_event_summary(summary);
        }
    }
}
