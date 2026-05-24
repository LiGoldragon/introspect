use introspect::command::IntrospectCommandLine;

fn main() -> introspect::Result<()> {
    IntrospectCommandLine::from_env().run(std::io::stdout())
}
