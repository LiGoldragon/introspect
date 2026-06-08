use introspect::surface::{Input, Output};
use signal_introspect::{
    ComponentReadiness, DeliveryTraceStatus, IntrospectionReply,
    PrototypeWitness as SignalPrototypeWitness,
};
use signal_persona::origin::EngineIdentifier;

#[test]
fn command_surface_uses_contract_text_codec() {
    let input = Input::from_nota("(PrototypeWitness ([prototype]))").expect("decode input");
    match input {
        Input::PrototypeWitness(query) => {
            assert_eq!(query.engine, EngineIdentifier::new("prototype"));
        }
    }

    let output = Output::from_signal(IntrospectionReply::PrototypeWitness(
        SignalPrototypeWitness {
            engine: EngineIdentifier::new("prototype"),
            manager_seen: None,
            router_seen: Some(ComponentReadiness::Ready),
            terminal_seen: None,
            delivery_status: Some(DeliveryTraceStatus::Routed),
        },
    ));

    assert_eq!(
        output.to_nota(),
        "(PrototypeWitness ([prototype] None (Some Ready) None (Some Routed)))",
    );
}
