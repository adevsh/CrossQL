pub mod metrics;

use crate::executor::engine::execute_pipeline;
use crate::storage::jobs::{JobQueue, JobQueueError};
use crate::server::metrics::{metrics_handler, MetricsRegistry};
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use crossql_shared::{JobRecord, NodeProgress, PipelineDefinition};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    api_key: Arc<String>,
    started_at: Instant,
    output_root: PathBuf,
    jobs: JobQueue,
    metrics: MetricsRegistry,
    running: Arc<RwLock<HashMap<String, CancellationToken>>>,
}

impl AppState {
    pub fn new(api_key: String, output_root: PathBuf) -> std::io::Result<Self> {
        Ok(Self {
            api_key: Arc::new(api_key),
            started_at: Instant::now(),
            output_root: output_root.clone(),
            jobs: JobQueue::new(&output_root)?,
            metrics: MetricsRegistry::new(),
            running: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    uptime_secs: u64,
}

#[derive(Serialize)]
struct StatusResponse {
    active_runs: u32,
    queued: u32,
    completed_total: u32,
}

#[derive(Serialize)]
struct SubmitResponse {
    job_id: String,
    queued_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct JobStatusResponse {
    job: JobRecord,
    node_progress: Vec<NodeProgress>,
}

#[derive(Serialize)]
struct CancelResponse {
    cancelled: bool,
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .route("/pipeline/submit", post(submit_handler))
        .route("/pipeline/:job_id/status", get(pipeline_status_handler))
        .route("/pipeline/:job_id", delete(cancel_handler))
        .route("/runs", get(runs_handler))
        .route("/runs/:job_id", get(run_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), api_key_auth))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn api_key_auth(State(state): State<AppState>, req: Request<Body>, next: Next) -> Response {
    let authorized = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v == state.api_key.as_str())
        .unwrap_or(false);

    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    next.run(req).await
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        uptime_secs: state.started_at.elapsed().as_secs(),
    })
}

async fn status_handler(State(state): State<AppState>) -> Json<StatusResponse> {
    let (active_runs, queued, completed_total) = state.jobs.counts().await;
    Json(StatusResponse {
        active_runs,
        queued,
        completed_total,
    })
}

async fn submit_handler(State(state): State<AppState>, body: Bytes) -> Result<Json<SubmitResponse>, StatusCode> {
    let pipeline: PipelineDefinition = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let job_id = Uuid::new_v4().to_string();
    let job = state
        .jobs
        .enqueue(job_id.clone(), &pipeline)
        .await
        .map_err(map_queue_error)?;

    let response_job_id = job_id.clone();
    let output_root = state.output_root.clone();
    let jobs = state.jobs.clone();
    let metrics = state.metrics.clone();
    let pipeline_for_task = pipeline.clone();
    let spawned_job_id = job_id.clone();
    let cancel_token = CancellationToken::new();
    {
        let mut running = state.running.write().await;
        running.insert(job_id.clone(), cancel_token.clone());
    }
    let running = state.running.clone();
    tokio::spawn(async move {
        if jobs.start(&spawned_job_id).await.is_err() {
            let mut running_guard = running.write().await;
            running_guard.remove(&spawned_job_id);
            return;
        }
        metrics.job_started(&pipeline_for_task.pipeline_id);
        match execute_pipeline(
            jobs.clone(),
            metrics.clone(),
            pipeline_for_task.clone(),
            output_root,
            spawned_job_id.clone(),
            cancel_token.clone(),
        )
        .await
        {
            Ok(outcome) => {
                let _ = jobs.complete(&spawned_job_id, Some(outcome.output_path)).await;
            }
            Err(err) => {
                if cancel_token.is_cancelled() {
                    let _ = jobs.cancel(&spawned_job_id).await;
                } else {
                    let _ = jobs.fail(&spawned_job_id, err).await;
                }
            }
        }
        if let Some((record, _)) = jobs.get(&spawned_job_id).await {
            metrics
                .job_finished(&record.pipeline_id, record.duration_secs)
                .await;
        }
        let mut running_guard = running.write().await;
        running_guard.remove(&spawned_job_id);
    });

    Ok(Json(SubmitResponse {
        job_id: response_job_id,
        queued_at: job.submitted_at,
    }))
}

async fn pipeline_status_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobStatusResponse>, StatusCode> {
    let (job, node_progress) = state.jobs.get(&job_id).await.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(JobStatusResponse { job, node_progress }))
}

async fn cancel_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Json<CancelResponse> {
    if let Some(token) = state.running.read().await.get(&job_id).cloned() {
        token.cancel();
    }
    let cancelled = state.jobs.cancel(&job_id).await.is_ok();
    Json(CancelResponse { cancelled })
}

async fn runs_handler(State(state): State<AppState>) -> Json<Vec<JobRecord>> {
    Json(state.jobs.list().await)
}

async fn run_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobRecord>, StatusCode> {
    let (job, _) = state.jobs.get(&job_id).await.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(job))
}

fn map_queue_error(err: JobQueueError) -> StatusCode {
    match err {
        JobQueueError::NotFound(_) => StatusCode::NOT_FOUND,
        JobQueueError::InvalidTransition { .. } => StatusCode::BAD_REQUEST,
        JobQueueError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::header::AUTHORIZATION;
    use axum::http::Method;
    use chrono::DateTime;
    use serde_json::Value;
    use std::time::Duration;
    use tower::ServiceExt;
    use uuid::Uuid;

    fn app() -> Router {
        let root = std::env::temp_dir().join(format!("crossql-server-test-{}", Uuid::new_v4()));
        build_app(AppState::new("test-key".to_string(), root).unwrap())
    }

    async fn request(method: Method, path: &str, auth: Option<&str>, body: Option<String>) -> Response {
        let mut req = Request::builder().method(method).uri(path);
        if let Some(value) = auth {
            req = req.header(AUTHORIZATION, value);
        }
        app()
            .oneshot(
                req.body(body.map(Body::from).unwrap_or_else(Body::empty))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn read_json(resp: Response) -> Value {
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_health_endpoint_returns_200() {
        let resp = request(Method::GET, "/health", Some("Bearer test-key"), None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json(resp).await;
        assert_eq!(body.get("status").unwrap(), "ok");
    }

    #[tokio::test]
    async fn test_health_endpoint_missing_key_returns_401() {
        let resp = request(Method::GET, "/health", None, None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_health_endpoint_wrong_key_returns_401() {
        let resp = request(Method::GET, "/health", Some("Bearer wrong-key"), None).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_status_endpoint_initial_state() {
        let resp = request(Method::GET, "/status", Some("Bearer test-key"), None).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json(resp).await;
        assert_eq!(body.get("active_runs").unwrap(), 0);
        assert_eq!(body.get("queued").unwrap(), 0);
        assert_eq!(body.get("completed_total").unwrap(), 0);
    }

    #[tokio::test]
    async fn test_uptime_increases_over_time() {
        let app = app();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let req = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .header(AUTHORIZATION, "Bearer test-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json(resp).await;
        assert!(body
            .get("uptime_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or_default()
            > 0);
    }

    #[tokio::test]
    async fn test_submit_bad_payload_returns_400() {
        let resp = request(
            Method::POST,
            "/pipeline/submit",
            Some("Bearer test-key"),
            Some("{bad json}".to_string()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_submit_returns_job_id() {
        let payload = serde_json::json!({
            "pipeline_id": "orders_join",
            "pipeline_version": 1,
            "nodes": [{"id":"n1","node_type":"source","config":{}}],
            "edges": [],
            "sources": {"s1":{"alias":"POSTGRES","source_type":"PostgreSQL","partition_key":null,"partition_hint":null}},
            "execution": {"chunk_size":50000,"max_concurrency":4,"streaming":true}
        })
        .to_string();
        let resp = request(
            Method::POST,
            "/pipeline/submit",
            Some("Bearer test-key"),
            Some(payload),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json(resp).await;
        assert!(body.get("job_id").is_some());
        assert!(Uuid::parse_str(body["job_id"].as_str().unwrap()).is_ok());
        assert!(DateTime::parse_from_rfc3339(body["queued_at"].as_str().unwrap()).is_ok());
    }
}
