use std::path::{Path, PathBuf};

use persona_introspect::runtime::{
    HandleIntrospectionRequest, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use persona_introspect::store::{IntrospectionStore, StoreLocation};
use signal_core::SignalVerb;
use signal_persona_auth::EngineId;
use signal_persona_introspect::{
    ComponentSnapshotQuery, CorrelationId, DeliveryTraceQuery, EngineSnapshotQuery,
    IntrospectionReply, IntrospectionRequest, IntrospectionTarget, PrototypeWitnessQuery,
};

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
        StoreLocation::new(self.directory.path().join("introspect.redb"))
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
        .block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: TargetSocketDirectory::empty(),
            store: fixture.store(),
        }))
        .expect("root starts");
    let request = IntrospectionRequest::PrototypeWitness(PrototypeWitnessQuery {
        engine: EngineId::new("prototype"),
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
    assert_eq!(operation.verb(), SignalVerb::Assert);
    assert_eq!(operation.table_name(), "introspection_observations");
    assert_eq!(operation.key().map(|key| key.as_str()), Some("1"));
}

#[test]
fn introspection_source_does_not_open_peer_component_redb_files() {
    let fixture = IntrospectionStoreFixture::new();
    let source = fixture.source_text();

    for forbidden in [
        "redb::Database::open",
        "router.redb",
        "terminal.redb",
        "mind.redb",
        "message.redb",
        "harness.redb",
    ] {
        assert!(
            !source.contains(forbidden),
            "persona-introspect source must not contain peer database path or open call: {forbidden}"
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
    let engine = EngineId::new("prototype");
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
            correlation: CorrelationId::new("fixture-delivery"),
        }),
        IntrospectionRequest::PrototypeWitness(PrototypeWitnessQuery {
            engine: engine.clone(),
        }),
    ];

    let root = runtime
        .block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: TargetSocketDirectory::empty(),
            store: fixture.store(),
        }))
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
        assert_eq!(operation.verb(), SignalVerb::Assert);
        assert_eq!(operation.table_name(), "introspection_observations");
    }
}

#[test]
fn introspect_daemon_does_not_depend_on_peer_component_contract_crates() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path).expect("manifest reads");

    for forbidden in [
        "signal-persona-router",
        "signal-persona-terminal",
        "signal-persona-message",
        "signal-persona-mind",
        "signal-persona-harness",
        "signal-persona-system",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "persona-introspect must not depend on peer contract crate: {forbidden} \
             (component observations are component-owned; introspect wraps, never redefines)"
        );
    }
}
