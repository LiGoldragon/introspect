//! Load-bearing slice test: the end-to-end tracing -> introspect path. An
//! emitting component PUSHES `ComponentTraceEvent` Signal frames over a Unix
//! trace socket; introspect's `ComponentTraceListener` PULLS them off the
//! socket into its sema store; an introspect-owned `ComponentTrace` query
//! returns them filtered by component and event name. No mentci, no spirit
//! dependency — the wire record is the shared `signal-introspect` contract
//! type both ends import.

use std::time::{Duration, Instant};

use introspect::runtime::{
    HandleIntrospectionQuery, IntrospectionRoot, IntrospectionRootInput, TargetSocketDirectory,
};
use introspect::store::StoreLocation;
use introspect::trace_frame::TracedComponentEvent;
use signal_introspect::{
    ComponentTraceEvent, ComponentTraceQuery, IntrospectionTarget, Query, Response, TraceLayer,
};
use triad_runtime::trace::TraceLog;

/// One Signal-layer trace event for the prototype engine at the given sequence.
fn signal_event(engine: &str, event_name: &str, sequence: i64) -> TracedComponentEvent {
    TracedComponentEvent::new(ComponentTraceEvent {
        engine_identifier: engine.to_owned(),
        introspection_target: IntrospectionTarget::Signal,
        trace_layer: TraceLayer::Signal,
        trace_event_name: event_name.to_owned(),
        trace_sequence: sequence,
    })
}

fn component_trace_query(engine: &str, event_name: Option<&str>) -> ComponentTraceQuery {
    ComponentTraceQuery {
        engine_identifier: engine.to_owned(),
        introspection_target: IntrospectionTarget::Signal,
        optional_trace_event_name: event_name.map(str::to_owned),
    }
}

/// Block until the trace socket the listener binds in `on_start` exists, so the
/// emitter's first `record` connects to a live listener rather than dropping.
fn await_socket(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "trace listener did not bind its socket within the deadline"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Drive a `ComponentTrace` query through the root actor, returning the events.
async fn query_component_trace(
    root: &kameo::actor::ActorRef<IntrospectionRoot>,
    query: ComponentTraceQuery,
) -> Vec<ComponentTraceEvent> {
    let response = root
        .ask(HandleIntrospectionQuery {
            query: Query::ComponentTrace(query),
        })
        .await
        .expect("root actor replies to component-trace query");
    match response {
        Response::ComponentTrace(trace) => trace.component_trace_events,
        other => panic!("expected ComponentTrace response, got {other:?}"),
    }
}

/// Poll the `ComponentTrace` query until the store has drained the expected
/// number of events, or the deadline passes. Ingestion is asynchronous (the
/// listener drains on a background blocking loop), so the query is the
/// observation point that proves the push reached durable state.
async fn await_drained(
    root: &kameo::actor::ActorRef<IntrospectionRoot>,
    query: ComponentTraceQuery,
    expected: usize,
) -> Vec<ComponentTraceEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = query_component_trace(root, query.clone()).await;
        if events.len() >= expected || Instant::now() >= deadline {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn pushed_signal_trace_events_are_ingested_and_queryable_by_component_and_name() {
    let directory = tempfile::tempdir().expect("tempdir");
    let trace_socket = directory.path().join("introspect-trace.sock");
    let store = StoreLocation::new(directory.path().join("introspect.sema"));

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let root = runtime
        .block_on(async {
            IntrospectionRoot::spawn_root(IntrospectionRootInput {
                targets: TargetSocketDirectory {
                    manager_socket: None,
                    router_socket: None,
                    terminal_socket: None,
                    trace_socket: Some(trace_socket.clone()),
                },
                store,
            })
        })
        .expect("root starts");

    // The listener binds its socket in on_start; wait for it before pushing so
    // the emitter connects to a live listener.
    await_socket(&trace_socket);

    // The emitting component pushes three Signal-layer events over the socket,
    // exactly as spirit's testing-trace sink does, using the shared contract
    // type. Sequence order is the monotonic emission order.
    let emitter = TraceLog::<TracedComponentEvent>::socket(&trace_socket);
    emitter
        .record_result(signal_event("prototype", "SignalStarted", 0))
        .expect("push SignalStarted");
    emitter
        .record_result(signal_event("prototype", "SignalAdmitted", 1))
        .expect("push SignalAdmitted");
    emitter
        .record_result(signal_event("prototype", "SignalReplied", 2))
        .expect("push SignalReplied");

    // Component-wide query (event name = None) returns all three in sequence
    // order once the listener has drained them into the store.
    let all_events = runtime.block_on(await_drained(
        &root,
        component_trace_query("prototype", None),
        3,
    ));
    assert_eq!(all_events.len(), 3, "all three pushed events were ingested");
    let names = all_events
        .iter()
        .map(|event| event.trace_event_name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "SignalStarted".to_owned(),
            "SignalAdmitted".to_owned(),
            "SignalReplied".to_owned(),
        ],
        "events return in monotonic sequence order"
    );
    let sequences = all_events
        .iter()
        .map(|event| event.trace_sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![0, 1, 2]);

    // Name-narrowed query returns exactly the one matching event.
    let admitted = runtime.block_on(query_component_trace(
        &root,
        component_trace_query("prototype", Some("SignalAdmitted")),
    ));
    assert_eq!(admitted.len(), 1, "event-name filter narrows to one event");
    assert_eq!(admitted[0].trace_event_name, "SignalAdmitted");
    assert_eq!(admitted[0].trace_sequence, 1);

    runtime
        .block_on(root.stop_gracefully())
        .expect("root stops gracefully");
    runtime.block_on(root.wait_for_shutdown());
}
