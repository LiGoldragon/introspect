use nota_config::ConfigurationSource;
use persona_introspect::Result;
use persona_introspect::daemon::IntrospectionDaemon;
use signal_persona_introspect::IntrospectDaemonConfiguration;

fn main() -> Result<()> {
    let configuration: IntrospectDaemonConfiguration =
        ConfigurationSource::from_argv()?.decode()?;
    IntrospectionDaemon::from_configuration(configuration).run()
}
