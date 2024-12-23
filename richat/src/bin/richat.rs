use {anyhow::Context, clap::Parser, richat::config::Config};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat App")]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.json"))]
    /// Path to config
    pub config: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    richat::log::setup(config.log.json)?;

    Ok(())
}
