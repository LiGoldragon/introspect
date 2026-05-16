use persona_introspect::runtime::{
    ExplainPrototypeWitness, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use persona_introspect::store::StoreLocation;
use signal_persona_auth::EngineId;
use signal_persona_introspect::{IntrospectionReply, PrototypeWitnessQuery};

#[test]
fn prototype_witness_uses_introspection_root_actor() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let directory = tempfile::tempdir().expect("tempdir");
    let root = runtime
        .block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
            targets: TargetSocketDirectory::empty(),
            store: StoreLocation::new(directory.path().join("introspect.redb")),
        }))
        .expect("root starts");
    let reply = runtime
        .block_on(async {
            root.ask(ExplainPrototypeWitness {
                query: PrototypeWitnessQuery {
                    engine: EngineId::new("prototype"),
                },
            })
            .await
        })
        .expect("actor reply");

    match reply {
        IntrospectionReply::PrototypeWitness(witness) => {
            assert_eq!(witness.engine, EngineId::new("prototype"));
            // Daemon skeleton has not yet collected peer observations;
            // every field is None per the closed-enum contract.
            assert_eq!(witness.delivery_status, None);
        }
        other => panic!("expected PrototypeWitness reply, got {other:?}"),
    }
}
