use introspect::Result;
use introspect::daemon::IntrospectionDaemon;
use nota_config::ConfigurationSource;
use signal_introspect::IntrospectDaemonConfiguration;

fn main() -> Result<()> {
    let configuration: IntrospectDaemonConfiguration =
        ConfigurationSource::from_argv()?.decode()?;
    IntrospectionDaemon::from_configuration(configuration).run()
}
