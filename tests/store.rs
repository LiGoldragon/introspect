use std::path::{Path, PathBuf};

use introspect::runtime::{
    HandleIntrospectionRequest, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use introspect::store::{IntrospectionStore, StoreLocation};
use sema_engine::RecordKey;
use signal_introspect::{
    ComponentSnapshotQuery, DeliveryTraceEvent, DeliveryTraceKey, DeliveryTraceQuery,
    DeliveryTraceStatus, EngineSnapshotQuery, HopIndex, IntrospectionReply, IntrospectionRequest,
    IntrospectionTarget, MessageIdentifier, PrototypeWitnessQuery,
};
use signal_persona::origin::{ComponentName, EngineIdentifier};

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

    fn source_root(&self) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
    }

    fn source_files(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.source_root())
            .expect("source directory reads")
            .map(|entry| entry.expect("source entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect()
    }

    fn source_text(&self) -> String {
        self.source_files()
            .into_iter()
            .map(|path| std::fs::read_to_string(path).expect("source file reads"))
            .collect::<Vec<_>>()
            .join("\n")
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
    let request = IntrospectionRequest::PrototypeWitness(PrototypeWitnessQuery {
        engine: EngineIdentifier::new("prototype"),
    });

    let reply = runtime
        .block_on(async {
            root.ask(HandleIntrospectionRequest {
                request: request.clone(),
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
    assert_eq!(observations[0].request(), &request);
    assert_eq!(observations[0].reply(), &reply);
    assert!(matches!(reply, IntrospectionReply::PrototypeWitness(_)));
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
fn introspection_source_does_not_open_peer_component_database_files() {
    let fixture = IntrospectionStoreFixture::new();
    let source = fixture.source_text();

    for forbidden in [
        "redb::Database::open",
        "router.sema",
        "terminal.sema",
        "mind.sema",
        "message.sema",
        "harness.sema",
    ] {
        assert!(
            !source.contains(forbidden),
            "introspect source must not contain peer database path or open call: {forbidden}"
        );
    }
}

#[test]
fn introspection_store_opens_local_state_through_sema_engine() {
    let fixture = IntrospectionStoreFixture::new();
    let store_source =
        std::fs::read_to_string(fixture.source_root().join("store.rs")).expect("store source");

    assert!(store_source.contains("Engine::open"));
    assert!(!store_source.contains("Sema::open_with_schema"));
    assert!(!store_source.contains("redb::Database::open"));
}

#[test]
fn every_introspection_request_variant_persists_through_actor_root_and_sema_engine() {
    let fixture = IntrospectionStoreFixture::new();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let engine = EngineIdentifier::new("prototype");
    let requests = [
        IntrospectionRequest::EngineSnapshot(EngineSnapshotQuery {
            engine: engine.clone(),
        }),
        IntrospectionRequest::ComponentSnapshot(ComponentSnapshotQuery {
            engine: engine.clone(),
            target: IntrospectionTarget::Router,
        }),
        IntrospectionRequest::DeliveryTrace(DeliveryTraceQuery {
            engine: engine.clone(),
            message_identifier: MessageIdentifier::new(7),
            originator: ComponentName::Message,
        }),
        IntrospectionRequest::PrototypeWitness(PrototypeWitnessQuery {
            engine: engine.clone(),
        }),
    ];

    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory::empty(),
                store: fixture.store(),
            })
        })
        .expect("root starts");

    let mut replies = Vec::with_capacity(requests.len());
    for request in &requests {
        let reply = runtime
            .block_on(async {
                root.ask(HandleIntrospectionRequest {
                    request: request.clone(),
                })
                .await
            })
            .expect("root actor replies");
        replies.push(reply);
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

    assert_eq!(observations.len(), requests.len());
    assert_eq!(operation_log.len(), requests.len());
    for (index, (request, reply)) in requests.iter().zip(replies.iter()).enumerate() {
        let observation = &observations[index];
        assert_eq!(observation.sequence().value() as usize, index + 1);
        assert_eq!(observation.request(), request);
        assert_eq!(observation.reply(), reply);
        let operation = operation_log[index].operations().head();
        assert_eq!(operation.operation().as_record_head(), "Assert");
        assert_eq!(operation.table_name(), "introspection_observations");
    }
}

#[test]
fn delivery_trace_query_returns_four_hops_ordered_by_trace_key() {
    use introspect::store::RecordDeliveryTraceEvent;

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
    let engine = EngineIdentifier::new("prototype");
    let message_identifier = MessageIdentifier::new(7);
    let originator = ComponentName::Message;
    let events = vec![
        trace_event(
            engine.clone(),
            message_identifier.clone(),
            originator,
            2,
            ComponentName::Router,
            DeliveryTraceStatus::Routed,
        ),
        trace_event(
            engine.clone(),
            message_identifier.clone(),
            originator,
            0,
            ComponentName::Message,
            DeliveryTraceStatus::Accepted,
        ),
        trace_event(
            engine.clone(),
            message_identifier.clone(),
            originator,
            3,
            ComponentName::Harness,
            DeliveryTraceStatus::Failed,
        ),
        trace_event(
            engine.clone(),
            message_identifier.clone(),
            originator,
            1,
            ComponentName::Mind,
            DeliveryTraceStatus::Routed,
        ),
    ];

    let noise = vec![
        trace_event(
            engine.clone(),
            MessageIdentifier::new(8),
            originator,
            0,
            ComponentName::Message,
            DeliveryTraceStatus::Accepted,
        ),
        trace_event(
            engine.clone(),
            message_identifier.clone(),
            ComponentName::Harness,
            0,
            ComponentName::Harness,
            DeliveryTraceStatus::Accepted,
        ),
    ];

    for event in events.into_iter().chain(noise) {
        runtime.block_on(async {
            root.ask(RecordDeliveryTraceEvent::new(event))
                .await
                .expect("store actor handles trace event")
        });
    }

    let request = IntrospectionRequest::DeliveryTrace(DeliveryTraceQuery {
        engine,
        message_identifier,
        originator,
    });
    let reply = runtime
        .block_on(async {
            root.ask(HandleIntrospectionRequest {
                request: request.clone(),
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

    let IntrospectionReply::DeliveryTrace(trace) = reply else {
        panic!("expected delivery trace reply");
    };
    assert_eq!(trace.events.len(), 4);
    let hops = trace
        .events
        .iter()
        .map(|event| event.key().hop_index.value())
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
fn introspect_daemon_depends_on_peer_contracts_not_peer_runtime_crates() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path).expect("manifest reads");

    assert!(
        manifest.contains("signal-router"),
        "RouterClient must speak the router observation contract through \
         signal-router rather than inventing a local copy"
    );

    for forbidden in ["router", "terminal", "message", "mind", "harness", "system"] {
        let direct_dependency_present = manifest
            .lines()
            .filter_map(|line| {
                line.split_once('=')
                    .map(|(dependency, _)| dependency.trim())
            })
            .any(|dependency| dependency == forbidden);
        assert!(
            !direct_dependency_present,
            "introspect must not depend on peer runtime crate: {forbidden} \
             (live observations cross daemon sockets; introspect does not call peer internals)"
        );
    }
}

fn trace_event(
    engine: EngineIdentifier,
    message_identifier: MessageIdentifier,
    originator: ComponentName,
    hop_index: u32,
    component: ComponentName,
    status: DeliveryTraceStatus,
) -> DeliveryTraceEvent {
    DeliveryTraceEvent::new(
        DeliveryTraceKey::new(
            engine,
            message_identifier,
            originator,
            HopIndex::new(hop_index),
        ),
        component,
        status,
    )
}
