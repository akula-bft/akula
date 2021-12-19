use structopt::StructOpt;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Akula RPC", about = "RPC server for Akula")]
pub struct Opt {
    #[structopt(long, env)]
    pub kv_address: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info,rpc=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    Ok(())
}
