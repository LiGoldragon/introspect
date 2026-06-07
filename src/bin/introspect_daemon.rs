use introspect::IntrospectDaemonCommand;
use introspect::Result;

fn main() -> Result<()> {
    IntrospectDaemonCommand::from_environment().run()
}
