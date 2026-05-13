use persona_introspect::daemon::IntrospectionDaemonCommandLine;

fn main() -> persona_introspect::Result<()> {
    IntrospectionDaemonCommandLine::from_env().run()
}
