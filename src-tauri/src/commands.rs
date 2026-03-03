use crate::connectors::postgres::PostgresConnector;
use crate::connectors::mysql::MysqlConnector;
use crate::connectors::mongodb::MongoConnector;
use crate::connectors::cassandra::CassandraConnector;
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
pub struct MongoConfig {
    uri: String,
    database: String,
    collection: String,
    filter: Option<String>,
    projection: Option<String>,
    flatten_depth: Option<usize>,
}

#[derive(serde::Deserialize)]
pub struct CassandraConfig {
    contact_points: String,
    keyspace: String,
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

#[derive(serde::Serialize)]
pub struct SchemaField {
    name: String,
    dtype: String,
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

#[tauri::command]
pub async fn run_mysql_to_parquet(
    source: PostgresConfig,
    output: ParquetConfig,
) -> Result<RunResult, String> {
    let df = MysqlConnector::fetch_dataframe(
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

#[tauri::command]
pub async fn preview_postgres_schema(source: PostgresConfig) -> Result<Vec<SchemaField>, String> {
    let fields = PostgresConnector::describe_schema(
        &source.host,
        source.port,
        &source.user,
        &source.password,
        &source.database,
        &source.query,
    )
    .await?;

    Ok(fields
        .into_iter()
        .map(|(name, dtype)| SchemaField { name, dtype })
        .collect())
}

#[tauri::command]
pub async fn preview_mysql_schema(source: PostgresConfig) -> Result<Vec<SchemaField>, String> {
    let fields = MysqlConnector::describe_schema(
        &source.host,
        source.port,
        &source.user,
        &source.password,
        &source.database,
        &source.query,
    )
    .await?;

    Ok(fields
        .into_iter()
        .map(|(name, dtype)| SchemaField { name, dtype })
        .collect())
}

#[tauri::command]
pub async fn run_mongodb_to_parquet(
    source: MongoConfig,
    output: ParquetConfig,
) -> Result<RunResult, String> {
    let df = MongoConnector::fetch_dataframe(
        &source.uri,
        &source.database,
        &source.collection,
        source.filter.as_deref().unwrap_or("{}"),
        source.projection.as_deref().unwrap_or("{}"),
        source.flatten_depth.unwrap_or(1),
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

#[tauri::command]
pub async fn preview_mongodb_schema(source: MongoConfig) -> Result<Vec<SchemaField>, String> {
    let fields = MongoConnector::describe_schema(
        &source.uri,
        &source.database,
        &source.collection,
        source.filter.as_deref().unwrap_or("{}"),
        source.projection.as_deref().unwrap_or("{}"),
        source.flatten_depth.unwrap_or(1),
    )
    .await?;

    Ok(fields
        .into_iter()
        .map(|(name, dtype)| SchemaField { name, dtype })
        .collect())
}

#[tauri::command]
pub async fn run_cassandra_to_parquet(
    source: CassandraConfig,
    output: ParquetConfig,
) -> Result<RunResult, String> {
    let df = CassandraConnector::fetch_dataframe(
        &source.contact_points,
        &source.keyspace,
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

#[tauri::command]
pub async fn preview_cassandra_schema(source: CassandraConfig) -> Result<Vec<SchemaField>, String> {
    let fields = CassandraConnector::describe_schema(
        &source.contact_points,
        &source.keyspace,
        &source.query,
    )
    .await?;

    Ok(fields
        .into_iter()
        .map(|(name, dtype)| SchemaField { name, dtype })
        .collect())
}
