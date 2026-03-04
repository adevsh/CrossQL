use crate::connectors::postgres::PostgresConnector;
use crate::connectors::mysql::MysqlConnector;
use crate::connectors::mongodb::MongoConnector;
use crate::connectors::cassandra::CassandraConnector;
use crate::engine::pipeline::{FlowEdge, FlowNode, NodeProgressFn, PipelineEngine, PipelineRunResult, PreviewResult, noop_progress};
use crate::run_manager::RunEntry;
use crate::run_manager::RunManager;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tauri::Emitter;

#[tauri::command]
pub fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[derive(serde::Deserialize)]
pub struct SqlSourceConfig {
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
    PipelineEngine::run(nodes, edges, true, noop_progress()).await
}

#[derive(serde::Serialize, Clone)]
pub struct PipelineEvent {
    run_id: String,
    kind: String,
    node_id: Option<String>,
    state: Option<String>,
    message: Option<String>,
    result: Option<serde_json::Value>,
}

fn emit_pipeline_event(app: &tauri::AppHandle, evt: PipelineEvent) {
    if let Err(e) = app.emit("pipeline_event", evt) {
        eprintln!("failed to emit pipeline_event: {}", e);
    }
}

#[tauri::command]
pub async fn start_pipeline_run(
    app: tauri::AppHandle,
    run_manager: tauri::State<'_, Arc<RunManager>>,
    nodes: Vec<FlowNode>,
    edges: Vec<FlowEdge>,
) -> Result<String, String> {
    let (run_id, entry) = run_manager.create_run().await;
    let run_id_clone = run_id.clone();
    let app_clone = app.clone();
    let entry_clone: Arc<RunEntry> = entry.clone();

    tauri::async_runtime::spawn(async move {
        emit_pipeline_event(
            &app_clone,
            PipelineEvent {
                run_id: run_id_clone.clone(),
                kind: "run_started".to_string(),
                node_id: None,
                state: None,
                message: None,
                result: None,
            },
        );

        // Per-node progress callback
        let progress_app = app_clone.clone();
        let progress_run_id = run_id_clone.clone();
        let on_progress: NodeProgressFn = Arc::new(move |node_id: &str, state: &str| {
            emit_pipeline_event(
                &progress_app,
                PipelineEvent {
                    run_id: progress_run_id.clone(),
                    kind: "node_state".to_string(),
                    node_id: Some(node_id.to_string()),
                    state: Some(state.to_string()),
                    message: None,
                    result: None,
                },
            );
        });

        let run_fut = PipelineEngine::run(nodes, edges, false, on_progress);
        let outcome = tokio::select! {
            _ = entry_clone.cancel.cancelled() => Err("Cancelled".to_string()),
            res = run_fut => res,
        };

        match outcome {
            Ok(result) => {
                let payload = serde_json::to_value(&result).ok();
                let _ = entry_clone
                    .set_result(payload.clone().ok_or_else(|| "Failed to serialize result".to_string()))
                    .await;
                emit_pipeline_event(
                    &app_clone,
                    PipelineEvent {
                        run_id: run_id_clone.clone(),
                        kind: "run_finished".to_string(),
                        node_id: None,
                        state: None,
                        message: None,
                        result: payload,
                    },
                );
            }
            Err(err) => {
                let kind = if err == "Cancelled" { "run_cancelled" } else { "run_error" };
                entry_clone.set_result(Err(err.clone())).await;
                emit_pipeline_event(
                    &app_clone,
                    PipelineEvent {
                        run_id: run_id_clone.clone(),
                        kind: kind.to_string(),
                        node_id: None,
                        state: None,
                        message: Some(err.clone()),
                        result: None,
                    },
                );
            }
        }
    });

    Ok(run_id)
}

#[tauri::command]
pub async fn await_pipeline_run(
    run_manager: tauri::State<'_, Arc<RunManager>>,
    runId: String,
) -> Result<serde_json::Value, String> {
    run_manager.await_run(&runId).await
}

#[tauri::command]
pub async fn cancel_pipeline_run(
    run_manager: tauri::State<'_, Arc<RunManager>>,
    runId: String,
) -> Result<(), String> {
    let ok = run_manager.cancel_run(&runId).await;
    if ok {
        Ok(())
    } else {
        Err("Run not found".to_string())
    }
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
pub async fn preview_postgres_schema(source: SqlSourceConfig) -> Result<Vec<SchemaField>, String> {
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
pub async fn preview_mysql_schema(source: SqlSourceConfig) -> Result<Vec<SchemaField>, String> {
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
