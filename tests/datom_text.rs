//! The component's text plane: a contract value written as datom reads back as
//! the same value, and a hand-typed request text actualizes into the contract
//! type the CLI submits.

use introspect::datom_text;
use meta_signal_introspect::{
    Configured, Query as MetaIntrospectQuery, Response as MetaIntrospectResponse,
};
use signal_introspect::{
    ComponentReadiness, DeliveryTraceObservationStatus, IntrospectDaemonConfiguration,
    PrototypeWitnessObservation, PrototypeWitnessObservationQuery, Query, Response,
};
use signal_persona::OwnerIdentity;

fn witness_response() -> Response {
    Response::PrototypeWitnessObservation(PrototypeWitnessObservation {
        engine_identifier: "prototype".to_owned(),
        first_optional_component_readiness: None,
        second_optional_component_readiness: Some(ComponentReadiness::Ready),
        third_optional_component_readiness: None,
        delivery_trace_observation_status_option: Some(DeliveryTraceObservationStatus::Routed),
    })
}

fn daemon_configuration() -> IntrospectDaemonConfiguration {
    IntrospectDaemonConfiguration {
        introspect_socket_path: "/tmp/introspect.sock".to_owned(),
        introspect_socket_mode: 0o600,
        supervision_socket_path: "/tmp/meta-introspect.sock".to_owned(),
        supervision_socket_mode: 0o600,
        store_path: "/tmp/introspect.sema".to_owned(),
        manager_socket_path: String::new(),
        router_socket_path: String::new(),
        terminal_socket_path: String::new(),
        trace_socket_path: String::new(),
        owner_identity: OwnerIdentity::UnixUser(1000),
    }
}

#[test]
fn an_introspection_query_round_trips_through_its_datom_text() {
    let query = Query::PrototypeWitnessObservation(PrototypeWitnessObservationQuery {
        engine_identifier: "prototype".to_owned(),
    });
    let text = datom_text::textualize(&query);
    let restored: Query =
        datom_text::actualize(&text).unwrap_or_else(|error| panic!("{text} actualizes: {error}"));
    assert_eq!(restored, query);
}

#[test]
fn an_introspection_response_round_trips_through_its_datom_text() {
    let response = witness_response();
    let text = datom_text::textualize(&response);
    let restored: Response =
        datom_text::actualize(&text).unwrap_or_else(|error| panic!("{text} actualizes: {error}"));
    assert_eq!(restored, response);
}

#[test]
fn a_meta_query_and_response_round_trip_through_their_datom_text() {
    let query = MetaIntrospectQuery::Configure(daemon_configuration());
    let text = datom_text::textualize(&query);
    let restored: MetaIntrospectQuery =
        datom_text::actualize(&text).unwrap_or_else(|error| panic!("{text} actualizes: {error}"));
    assert_eq!(restored, query);

    let response = MetaIntrospectResponse::Configured(Configured {
        configuration_generation: 3,
    });
    let text = datom_text::textualize(&response);
    let restored: MetaIntrospectResponse =
        datom_text::actualize(&text).unwrap_or_else(|error| panic!("{text} actualizes: {error}"));
    assert_eq!(restored, response);
}

#[test]
fn a_hand_typed_request_text_actualizes_into_the_contract_query() {
    let query: Query = datom_text::actualize("PrototypeWitnessObservation.{ prototype }")
        .expect("hand-typed prototype witness request actualizes");
    assert_eq!(
        query,
        Query::PrototypeWitnessObservation(PrototypeWitnessObservationQuery {
            engine_identifier: "prototype".to_owned(),
        })
    );
}

#[test]
fn a_malformed_request_text_faults_with_its_datom_layer_and_path() {
    let error = datom_text::actualize::<Query>("PrototypeWitnessObservation.{ }")
        .expect_err("an empty struct body cannot fill a one-position anatomy");
    let rendered = error.to_string();
    assert!(rendered.starts_with("datom text: "), "{rendered}");
}
