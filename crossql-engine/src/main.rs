use crossql_engine::server::{build_app, AppState};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crossql_engine=info,tower_http=info".into()),
        )
        .init();

    let api_key = env::var("CROSSQL_API_KEY").map_err(|_| "CROSSQL_API_KEY is required")?;
    let output_dir =
        env::var("CROSSQL_OUTPUT_DIR").map_err(|_| "CROSSQL_OUTPUT_DIR is required")?;
    let port = env::var("CROSSQL_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(7070);

    let state = AppState::new(api_key, PathBuf::from(output_dir.clone()))?;
    let app = build_app(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    info!(port = %port, output_dir = %output_dir, "crossql-engine starting");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
