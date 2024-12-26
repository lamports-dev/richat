use {
    std::io::{self, IsTerminal},
    tracing::Subscriber,
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        fmt::layer,
        layer::{Layer, SubscriberExt},
        registry::LookupSpan,
        util::SubscriberInitExt,
    },
};

pub fn setup(json: bool) -> anyhow::Result<()> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    tracing_subscriber::registry()
        .with(env)
        .with(create_io_layer(json))
        .try_init()?;

    Ok(())
}

fn create_io_layer<S>(json: bool) -> Box<dyn Layer<S> + Send + Sync + 'static>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = layer().with_ansi(is_atty).with_line_number(true);

    if json {
        Box::new(io_layer.json())
    } else {
        Box::new(io_layer)
    }
}
