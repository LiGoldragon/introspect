use introspect::IntrospectionDaemon;
use introspect::schema::daemon::DaemonEntry;

fn main() -> std::process::ExitCode {
    <IntrospectionDaemon as DaemonEntry>::run_to_exit_code()
}
