use crate::connectors::postgres::PostgresConnector;
use std::fs;

#[tauri::command]
pub fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[derive(serde::Deserialize)]
pub struct PostgresConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    query: String,
}

#[derive(serde::Deserialize)]
pub struct ParquetConfig {
    path: String,
    compression: String,
    row_group_size: Option<usize>,
}

#[derive(serde::Serialize)]
pub struct RunResult {
    row_count: usize,
    path: String,
    file_size_bytes: u64,
}

#[tauri::command]
pub async fn run_postgres_to_parquet(
    source: PostgresConfig,
    output: ParquetConfig,
) -> Result<RunResult, String> {
    let df = PostgresConnector::fetch_dataframe(
        &source.host,
        source.port,
        &source.user,
        &source.password,
        &source.database,
        &source.query,
    )
    .await?;

    let row_count = df.height();

    crate::writer::parquet::CrossQLParquetWriter::write_dataframe(
        df,
        &output.path,
        &output.compression,
        output.row_group_size,
    )?;

    let file_size_bytes = fs::metadata(&output.path)
        .map_err(|e| format!("Failed to stat output file: {}", e))?
        .len();

    Ok(RunResult {
        row_count,
        path: output.path,
        file_size_bytes,
    })
}
