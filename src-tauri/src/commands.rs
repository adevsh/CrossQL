use crate::connectors::postgres::PostgresConnector;
use crate::connectors::mysql::MysqlConnector;
use crate::connectors::mongodb::MongoConnector;
use crate::connectors::cassandra::CassandraConnector;
use crate::engine::pipeline::{FlowEdge, FlowNode, PipelineEngine, PipelineRunResult, PreviewResult};
use crate::engine::schema::{apply_schema_maps, validate_no_nulls, SchemaMapConfig};
use polars::prelude::IntoLazy;
use std::fs;
use std::sync::{Mutex, OnceLock};
use sysinfo::{Pid, ProcessesToUpdate, System};

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

static SYSINFO: OnceLock<Mutex<System>> = OnceLock::new();

#[derive(serde::Serialize)]
pub struct ProcessUsage {
    cpu_percent: f32,
    memory_bytes: u64,
}

#[tauri::command]
pub fn get_process_usage() -> Result<ProcessUsage, String> {
    let pid = Pid::from_u32(std::process::id());
    let sys = SYSINFO.get_or_init(|| Mutex::new(System::new_all()));
    let mut sys = sys
        .lock()
        .map_err(|_| "Failed to lock process monitor".to_string())?;

    let pids = [pid];
    sys.refresh_processes(ProcessesToUpdate::Some(&pids), false);

    let p = sys
        .process(pid)
        .ok_or_else(|| "Process not found".to_string())?;

    Ok(ProcessUsage {
        cpu_percent: p.cpu_usage(),
        memory_bytes: p.memory(),
    })
}

#[tauri::command]
pub async fn run_pipeline(nodes: Vec<FlowNode>, edges: Vec<FlowEdge>) -> Result<PipelineRunResult, String> {
    PipelineEngine::run(nodes, edges).await
}

#[tauri::command]
pub async fn preview_pipeline_node(
    nodes: Vec<FlowNode>,
    edges: Vec<FlowEdge>,
    node_id: String,
) -> Result<PreviewResult, String> {
    PipelineEngine::preview_node(nodes, edges, node_id).await
}

#[tauri::command]
pub async fn run_postgres_to_parquet(
    source: PostgresConfig,
    output: ParquetConfig,
    schema_maps: Option<Vec<SchemaMapConfig>>,
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

    let mut lf = df.lazy();
    let mut error_on_null_cols: Vec<String> = Vec::new();
    if let Some(schema_maps) = schema_maps.as_ref() {
        let (next, err_cols) = apply_schema_maps(lf, schema_maps)?;
        lf = next;
        error_on_null_cols = err_cols;
    }

    let df = lf.collect().map_err(|e| format!("Failed to execute lazy pipeline: {}", e))?;
    validate_no_nulls(&df, &error_on_null_cols)?;
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
    schema_maps: Option<Vec<SchemaMapConfig>>,
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

    let mut lf = df.lazy();
    let mut error_on_null_cols: Vec<String> = Vec::new();
    if let Some(schema_maps) = schema_maps.as_ref() {
        let (next, err_cols) = apply_schema_maps(lf, schema_maps)?;
        lf = next;
        error_on_null_cols = err_cols;
    }

    let df = lf.collect().map_err(|e| format!("Failed to execute lazy pipeline: {}", e))?;
    validate_no_nulls(&df, &error_on_null_cols)?;
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
    schema_maps: Option<Vec<SchemaMapConfig>>,
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

    let mut lf = df.lazy();
    let mut error_on_null_cols: Vec<String> = Vec::new();
    if let Some(schema_maps) = schema_maps.as_ref() {
        let (next, err_cols) = apply_schema_maps(lf, schema_maps)?;
        lf = next;
        error_on_null_cols = err_cols;
    }

    let df = lf.collect().map_err(|e| format!("Failed to execute lazy pipeline: {}", e))?;
    validate_no_nulls(&df, &error_on_null_cols)?;
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
    schema_maps: Option<Vec<SchemaMapConfig>>,
) -> Result<RunResult, String> {
    let df = CassandraConnector::fetch_dataframe(
        &source.contact_points,
        &source.keyspace,
        &source.query,
    )
    .await?;

    let mut lf = df.lazy();
    let mut error_on_null_cols: Vec<String> = Vec::new();
    if let Some(schema_maps) = schema_maps.as_ref() {
        let (next, err_cols) = apply_schema_maps(lf, schema_maps)?;
        lf = next;
        error_on_null_cols = err_cols;
    }

    let df = lf.collect().map_err(|e| format!("Failed to execute lazy pipeline: {}", e))?;
    validate_no_nulls(&df, &error_on_null_cols)?;
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
