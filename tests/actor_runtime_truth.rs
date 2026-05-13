use persona_introspect::runtime::{
    ExplainPrototypeWitness, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use signal_persona_auth::EngineId;
use signal_persona_introspect::{DeliveryTraceStatus, IntrospectionReply, PrototypeWitnessQuery};

#[test]
fn prototype_witness_uses_introspection_root_actor() {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime.block_on(IntrospectionRoot::start_root(IntrospectionRootInput {
        targets: TargetSocketDirectory::empty(),
    }));
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
            assert_eq!(witness.delivery_status, DeliveryTraceStatus::Unknown);
        }
        other => panic!("expected PrototypeWitness reply, got {other:?}"),
    }
}
