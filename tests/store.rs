//! Introspect's durable state: what the root records, what survives an additive
//! table migration, what bounded retention reclaims, and how a delivery trace
//! is ordered.

use introspect::runtime::{
    HandleIntrospectionQuery, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use introspect::store::{IntrospectionStore, PersistenceRetention, StoreLocation};
use introspect::store_message::RecordDeliveryTraceEvent;
use introspect::store_record::{ObservationSequence, StoredObservation};
use sema_engine::{
    Assertion, Engine, EngineOpen, FamilyName, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, VersionedStoreName, VersioningPolicy,
};
use signal_introspect::{
    ComponentSnapshotObservationQuery, ComponentTraceEvent, ComponentTraceQuery,
    DeliveryTraceObservationEvent, DeliveryTraceObservationKey, DeliveryTraceObservationQuery,
    DeliveryTraceObservationStatus, EngineSnapshotObservation, EngineSnapshotObservationQuery,
    IntrospectionTarget, PrototypeWitnessObservationQuery, Query, Response, TraceLayer,
};

const ENGINE: &str = "prototype";

struct IntrospectionStoreFixture {
    directory: tempfile::TempDir,
}

impl IntrospectionStoreFixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("tempdir"),
        }
    }

    fn store(&self) -> StoreLocation {
        StoreLocation::new(self.directory.path().join("introspect.sema"))
    }
}

fn engine_snapshot_query() -> Query {
    Query::EngineSnapshotObservation(EngineSnapshotObservationQuery {
        engine_identifier: ENGINE.to_owned(),
    })
}

fn prototype_witness_query() -> Query {
    Query::PrototypeWitnessObservation(PrototypeWitnessObservationQuery {
        engine_identifier: ENGINE.to_owned(),
    })
}

fn delivery_trace_query(message_slot: i64, originator: &str) -> DeliveryTraceObservationQuery {
    DeliveryTraceObservationQuery {
        engine_identifier: ENGINE.to_owned(),
        message_slot,
        component_name: originator.to_owned(),
    }
}

fn trace_event(
    message_slot: i64,
    originator: &str,
    hop_index: i64,
    component: &str,
    status: DeliveryTraceObservationStatus,
) -> DeliveryTraceObservationEvent {
    DeliveryTraceObservationEvent {
        delivery_trace_observation_key: DeliveryTraceObservationKey {
            engine_identifier: ENGINE.to_owned(),
            message_slot,
            component_name: originator.to_owned(),
            hop_index,
        },
        component_name: component.to_owned(),
        delivery_trace_observation_status: status,
    }
}

fn component_trace_event(sequence: i64) -> ComponentTraceEvent {
    ComponentTraceEvent {
        engine_identifier: ENGINE.to_owned(),
        introspection_target: IntrospectionTarget::Signal,
        trace_layer: TraceLayer::Signal,
        trace_event_name: "SignalAdmitted".to_owned(),
        trace_sequence: sequence,
    }
}

#[test]
fn introspection_root_records_observations_through_sema_engine() {
    let fixture = IntrospectionStoreFixture::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory::empty(),
                store: fixture.store(),
            })
        })
        .expect("root starts");
    let query = prototype_witness_query();

    let response = runtime
        .block_on(async {
            root.ask(HandleIntrospectionQuery {
                query: query.clone(),
            })
            .await
        })
        .expect("root actor replies");

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops gracefully");
    runtime.block_on(root.wait_for_shutdown());
    drop(root);
    drop(runtime);

    let store = IntrospectionStore::open(&fixture.store()).expect("store reopens");
    let observations = store.observations().expect("observations read");
    let operation_log = store.operation_log().expect("operation log reads");

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].sequence().value(), 1);
    assert_eq!(observations[0].query(), &query);
    assert_eq!(observations[0].response(), &response);
    assert!(matches!(response, Response::PrototypeWitnessObservation(_)));
    assert_eq!(operation_log.len(), 1);
    let operation = operation_log[0].operations().head();
    assert_eq!(operation.operation().as_record_head(), "Assert");
    assert_eq!(operation.table_name(), "introspection_observations");
    assert!(matches!(
        operation.key(),
        Some(RecordKey::Domain(key)) if key == "1"
    ));
}

#[test]
fn additive_system_event_table_migration_keeps_version_three_observations_readable() {
    let fixture = IntrospectionStoreFixture::new();
    let location = fixture.store();
    let mut legacy_engine = Engine::open(
        EngineOpen::new(location.as_path().to_path_buf(), SchemaVersion::new(3))
            .with_versioning(VersioningPolicy::new(VersionedStoreName::new("introspect"))),
    )
    .expect("legacy version-three store opens");
    let observations = legacy_engine
        .register_table(TableDescriptor::<StoredObservation>::new(
            TableName::new("introspection_observations"),
            FamilyName::new("introspection-observation"),
            SchemaHash::for_label("introspect-introspection-observation-v3"),
        ))
        .expect("legacy observation table registers");
    let observation = StoredObservation::new(
        ObservationSequence::new(1),
        engine_snapshot_query(),
        Response::EngineSnapshotObservation(EngineSnapshotObservation {
            engine_identifier: ENGINE.to_owned(),
            observed_components: Vec::new(),
        }),
    );
    legacy_engine
        .assert(Assertion::new(observations, observation.clone()))
        .expect("legacy observation persists");
    drop(legacy_engine);

    let migrated = IntrospectionStore::open(&location)
        .expect("additive system-event family registers on legacy store");
    assert_eq!(
        migrated.observations().expect("observations read"),
        vec![observation]
    );
}

#[test]
fn observation_query_families_persist_through_actor_root_and_sema_engine() {
    let fixture = IntrospectionStoreFixture::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let queries = [
        engine_snapshot_query(),
        Query::ComponentSnapshotObservation(ComponentSnapshotObservationQuery {
            engine_identifier: ENGINE.to_owned(),
            introspection_target: IntrospectionTarget::Router,
        }),
        Query::DeliveryTraceObservation(delivery_trace_query(7, "Message")),
        prototype_witness_query(),
    ];

    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory::empty(),
                store: fixture.store(),
            })
        })
        .expect("root starts");

    let mut responses = Vec::with_capacity(queries.len());
    for query in &queries {
        let response = runtime
            .block_on(async {
                root.ask(HandleIntrospectionQuery {
                    query: query.clone(),
                })
                .await
            })
            .expect("root actor replies");
        responses.push(response);
    }

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops gracefully");
    runtime.block_on(root.wait_for_shutdown());
    drop(root);
    drop(runtime);

    let store = IntrospectionStore::open(&fixture.store()).expect("store reopens");
    let observations = store.observations().expect("observations read");
    let operation_log = store.operation_log().expect("operation log reads");

    assert_eq!(observations.len(), queries.len());
    assert_eq!(operation_log.len(), queries.len());
    for (index, (query, response)) in queries.iter().zip(responses.iter()).enumerate() {
        let observation = &observations[index];
        assert_eq!(observation.sequence().value() as usize, index + 1);
        assert_eq!(observation.query(), query);
        assert_eq!(observation.response(), response);
        let operation = operation_log[index].operations().head();
        assert_eq!(operation.operation().as_record_head(), "Assert");
        assert_eq!(operation.table_name(), "introspection_observations");
    }
}

#[test]
fn delivery_trace_query_returns_four_hops_ordered_by_trace_key() {
    let fixture = IntrospectionStoreFixture::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory::empty(),
                store: fixture.store(),
            })
        })
        .expect("root starts");
    let events = vec![
        trace_event(
            7,
            "Message",
            2,
            "Router",
            DeliveryTraceObservationStatus::Routed,
        ),
        trace_event(
            7,
            "Message",
            0,
            "Message",
            DeliveryTraceObservationStatus::Accepted,
        ),
        trace_event(
            7,
            "Message",
            3,
            "Harness",
            DeliveryTraceObservationStatus::Failed,
        ),
        trace_event(
            7,
            "Message",
            1,
            "Mind",
            DeliveryTraceObservationStatus::Routed,
        ),
    ];

    // Hops of a different message, and of a different originator on the same
    // message, must not appear in the queried trace.
    let noise = vec![
        trace_event(
            8,
            "Message",
            0,
            "Message",
            DeliveryTraceObservationStatus::Accepted,
        ),
        trace_event(
            7,
            "Harness",
            0,
            "Harness",
            DeliveryTraceObservationStatus::Accepted,
        ),
    ];

    for event in events.into_iter().chain(noise) {
        runtime.block_on(async {
            root.ask(RecordDeliveryTraceEvent::new(event))
                .await
                .expect("store actor handles trace event")
        });
    }

    let response = runtime
        .block_on(async {
            root.ask(HandleIntrospectionQuery {
                query: Query::DeliveryTraceObservation(delivery_trace_query(7, "Message")),
            })
            .await
        })
        .expect("root actor replies");

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops gracefully");
    runtime.block_on(root.wait_for_shutdown());
    drop(root);
    drop(runtime);

    let Response::DeliveryTraceObservation(trace) = response else {
        panic!("expected delivery trace response");
    };
    assert_eq!(trace.delivery_trace_observation_events.len(), 4);
    let hops = trace
        .delivery_trace_observation_events
        .iter()
        .map(|event| event.delivery_trace_observation_key.hop_index)
        .collect::<Vec<_>>();
    assert_eq!(hops, vec![0, 1, 2, 3]);

    let store = IntrospectionStore::open(&fixture.store()).expect("store reopens");
    let operation_log = store.operation_log().expect("operation log reads");
    assert_eq!(operation_log.len(), 7);
    for operation in operation_log.iter().take(6) {
        let operation = operation.operations().head();
        assert_eq!(operation.operation().as_record_head(), "Assert");
        assert_eq!(operation.table_name(), "delivery_trace_events");
    }
}

#[test]
fn bounded_observation_retention_reclaims_oldest_rows() {
    let fixture = IntrospectionStoreFixture::new();
    let store = IntrospectionStore::open_with_retention(
        &fixture.store(),
        PersistenceRetention::new(2, 2, 2),
    )
    .expect("store opens with a bounded retention policy");

    for sequence in 1..=3 {
        store
            .record_observation(StoredObservation::new(
                ObservationSequence::new(sequence),
                engine_snapshot_query(),
                Response::EngineSnapshotObservation(EngineSnapshotObservation {
                    engine_identifier: ENGINE.to_owned(),
                    observed_components: Vec::new(),
                }),
            ))
            .expect("observation persists");
    }

    let retained = store.observations().expect("retained observations read");
    let sequences = retained
        .iter()
        .map(|observation| observation.sequence().value())
        .collect::<Vec<_>>();
    assert_eq!(
        sequences,
        vec![2, 3],
        "oldest diagnostic rows are retracted"
    );
}

#[test]
fn bounded_trace_retention_keeps_a_finite_queryable_window() {
    let fixture = IntrospectionStoreFixture::new();
    let store = IntrospectionStore::open_with_retention(
        &fixture.store(),
        PersistenceRetention::new(2, 2, 2),
    )
    .expect("store opens with a bounded retention policy");

    for sequence in 1..=3 {
        store
            .record_delivery_trace_event(trace_event(
                sequence,
                "Message",
                0,
                "Router",
                DeliveryTraceObservationStatus::Routed,
            ))
            .expect("delivery trace persists");
        store
            .record_component_trace_event(component_trace_event(sequence))
            .expect("component trace persists");
    }

    let first_delivery = store
        .delivery_trace(delivery_trace_query(1, "Message"))
        .expect("delivery trace query succeeds");
    assert!(
        first_delivery.delivery_trace_observation_events.is_empty(),
        "oldest delivery trace is reclaimed"
    );

    let traces = store
        .component_trace(ComponentTraceQuery {
            engine_identifier: ENGINE.to_owned(),
            introspection_target: IntrospectionTarget::Signal,
            optional_trace_event_name: None,
        })
        .expect("component trace query succeeds");
    let retained_sequences = traces
        .component_trace_events
        .iter()
        .map(|event| event.trace_sequence)
        .collect::<Vec<_>>();
    assert_eq!(
        retained_sequences,
        vec![2, 3],
        "oldest component trace is reclaimed"
    );
}
