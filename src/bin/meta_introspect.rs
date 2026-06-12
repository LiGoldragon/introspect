use introspect::meta::MetaIntrospectCommand;

fn main() -> introspect::Result<()> {
    MetaIntrospectCommand::from_env().run(std::io::stdout())
}
