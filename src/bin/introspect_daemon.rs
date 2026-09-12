use introspect::IntrospectionDaemon;
use introspect::daemon_shell::DaemonEntry;

fn main() -> std::process::ExitCode {
    <IntrospectionDaemon as DaemonEntry>::run_to_exit_code()
}
