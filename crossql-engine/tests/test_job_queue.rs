use axum::body::{to_bytes, Body};
use axum::http::header::AUTHORIZATION;
use axum::http::{Method, Request, StatusCode};
use crossql_engine::server::{build_app, AppState};
use crossql_engine::storage::jobs::JobQueue;
use crossql_shared::{ExecutionConfig, PipelineDefinition, PipelineNode};
use std::collections::HashMap;
use tower::ServiceExt;
use uuid::Uuid;

fn temp_output_root() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("crossql-job-queue-it-{}", Uuid::new_v4()))
}

fn pipeline() -> PipelineDefinition {
    PipelineDefinition {
        pipeline_id: "orders_join".to_string(),
        pipeline_version: 3,
        nodes: vec![
            PipelineNode {
                id: "n1".to_string(),
                node_type: "source".to_string(),
                config: serde_json::json!({}),
            },
            PipelineNode {
                id: "n2".to_string(),
                node_type: "transform".to_string(),
                config: serde_json::json!({}),
            },
        ],
        edges: vec![],
        sources: HashMap::new(),
        execution: ExecutionConfig::default(),
    }
}

fn pipeline_json() -> String {
    serde_json::to_string(&pipeline()).unwrap()
}

async fn request(
    app: axum::Router,
    method: Method,
    path: &str,
    body: Option<String>,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(method)
            .uri(path)
            .header(AUTHORIZATION, "Bearer test-key")
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn response_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn test_runs_json_written_on_status_change() {
    let root = temp_output_root();
    let queue = JobQueue::new(&root).unwrap();
    let def = pipeline();
    queue.enqueue("job-1".to_string(), &def).await.unwrap();
    queue.start("job-1").await.unwrap();
    queue.complete("job-1", None).await.unwrap();
    let body = std::fs::read_to_string(root.join("runs.json")).unwrap();
    assert!(body.contains("\"job_id\": \"job-1\""));
    assert!(body.contains("\"status\": \"Completed\""));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_runs_json_reloaded_on_startup() {
    let root = temp_output_root();
    let queue = JobQueue::new(&root).unwrap();
    queue.enqueue("job-1".to_string(), &pipeline()).await.unwrap();
    drop(queue);

    let queue_reloaded = JobQueue::new(&root).unwrap();
    let jobs = queue_reloaded.list().await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "job-1");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_submit_endpoint_returns_job_id() {
    let root = temp_output_root();
    let app = build_app(AppState::new("test-key".to_string(), root.clone()).unwrap());
    let resp = request(app, Method::POST, "/pipeline/submit", Some(pipeline_json())).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert!(Uuid::parse_str(body["job_id"].as_str().unwrap()).is_ok());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_submit_invalid_payload_returns_400() {
    let root = temp_output_root();
    let app = build_app(AppState::new("test-key".to_string(), root.clone()).unwrap());
    let resp = request(
        app,
        Method::POST,
        "/pipeline/submit",
        Some("{not-valid-json}".to_string()),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_get_pipeline_status_unknown_job_returns_404() {
    let root = temp_output_root();
    let app = build_app(AppState::new("test-key".to_string(), root.clone()).unwrap());
    let resp = request(app, Method::GET, "/pipeline/unknown/status", None).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_delete_pipeline_cancels_running_job() {
    let root = temp_output_root();
    let app = build_app(AppState::new("test-key".to_string(), root.clone()).unwrap());
    let submit = request(
        app.clone(),
        Method::POST,
        "/pipeline/submit",
        Some(pipeline_json()),
    )
    .await;
    let submit_body = response_json(submit).await;
    let job_id = submit_body["job_id"].as_str().unwrap();

    let resp = request(
        app,
        Method::DELETE,
        &format!("/pipeline/{}", job_id),
        None,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert_eq!(body["cancelled"], true);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn test_get_runs_returns_list() {
    let root = temp_output_root();
    let app = build_app(AppState::new("test-key".to_string(), root.clone()).unwrap());
    let _ = request(
        app.clone(),
        Method::POST,
        "/pipeline/submit",
        Some(pipeline_json()),
    )
    .await;
    let resp = request(app, Method::GET, "/runs", None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = response_json(resp).await;
    assert!(body.is_array());
    assert!(!body.as_array().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(root);
}
