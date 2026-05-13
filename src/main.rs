use persona_introspect::command::IntrospectCommandLine;

fn main() -> persona_introspect::Result<()> {
    IntrospectCommandLine::from_env().run(std::io::stdout())
}
