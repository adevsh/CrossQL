use super::AppState;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    active_pipelines: AtomicU64,
    memory_bytes_used: AtomicU64,
    chunks_processed_by_source: RwLock<HashMap<String, u64>>,
    duration_sum_by_pipeline: RwLock<HashMap<String, f64>>,
    duration_count_by_pipeline: RwLock<HashMap<String, u64>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn job_started(&self, _pipeline_id: &str) {
        self.inner.active_pipelines.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn job_finished(&self, pipeline_id: &str, duration_secs: Option<f64>) {
        self.inner
            .active_pipelines
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
            .ok();

        if let Some(duration) = duration_secs {
            let mut sums = self.inner.duration_sum_by_pipeline.write().await;
            let mut counts = self.inner.duration_count_by_pipeline.write().await;
            *sums.entry(pipeline_id.to_string()).or_insert(0.0) += duration;
            *counts.entry(pipeline_id.to_string()).or_insert(0) += 1;
        }
    }

    pub async fn increment_chunks_processed(&self, source: &str, by: u64) {
        let mut chunks = self.inner.chunks_processed_by_source.write().await;
        *chunks.entry(source.to_string()).or_insert(0) += by;
    }

    pub fn set_memory_bytes_used(&self, bytes: u64) {
        self.inner.memory_bytes_used.store(bytes, Ordering::Relaxed);
    }

    pub async fn render_prometheus(&self) -> String {
        let active = self.inner.active_pipelines.load(Ordering::Relaxed);
        let memory = self.inner.memory_bytes_used.load(Ordering::Relaxed);
        let chunks = self.inner.chunks_processed_by_source.read().await;
        let sums = self.inner.duration_sum_by_pipeline.read().await;
        let counts = self.inner.duration_count_by_pipeline.read().await;

        let mut body = String::new();
        body.push_str("# HELP crossql_pipeline_duration_seconds Duration of completed pipelines\n");
        body.push_str("# TYPE crossql_pipeline_duration_seconds histogram\n");
        if sums.is_empty() {
            body.push_str("crossql_pipeline_duration_seconds_sum{pipeline=\"none\"} 0\n");
            body.push_str("crossql_pipeline_duration_seconds_count{pipeline=\"none\"} 0\n");
        } else {
            for (pipeline, sum) in sums.iter() {
                let count = counts.get(pipeline).copied().unwrap_or_default();
                body.push_str(&format!(
                    "crossql_pipeline_duration_seconds_sum{{pipeline=\"{}\"}} {}\n",
                    pipeline, sum
                ));
                body.push_str(&format!(
                    "crossql_pipeline_duration_seconds_count{{pipeline=\"{}\"}} {}\n",
                    pipeline, count
                ));
            }
        }
        body.push('\n');
        body.push_str("# HELP crossql_chunks_processed_total Total chunks processed\n");
        body.push_str("# TYPE crossql_chunks_processed_total counter\n");
        if chunks.is_empty() {
            body.push_str("crossql_chunks_processed_total{source=\"unknown\"} 0\n");
        } else {
            for (source, value) in chunks.iter() {
                body.push_str(&format!(
                    "crossql_chunks_processed_total{{source=\"{}\"}} {}\n",
                    source, value
                ));
            }
        }
        body.push('\n');
        body.push_str("# HELP crossql_active_pipelines Currently running pipelines\n");
        body.push_str("# TYPE crossql_active_pipelines gauge\n");
        body.push_str(&format!("crossql_active_pipelines {}\n", active));
        body.push('\n');
        body.push_str("# HELP crossql_memory_bytes_used Approximate memory used by engine\n");
        body.push_str("# TYPE crossql_memory_bytes_used gauge\n");
        body.push_str(&format!("crossql_memory_bytes_used {}\n", memory));
        body
    }
}

pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.render_prometheus().await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4"))],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::build_app;
    use axum::body::{Body, to_bytes};
    use axum::http::header::AUTHORIZATION;
    use axum::http::{Method, Request};
    use tower::ServiceExt;
    use uuid::Uuid;

    fn app_state() -> AppState {
        let root = std::env::temp_dir().join(format!("crossql-metrics-test-{}", Uuid::new_v4()));
        AppState::new("test-key".to_string(), root).unwrap()
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_200() {
        let app = build_app(app_state());
        let req = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .header(AUTHORIZATION, "Bearer test-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_response_is_valid_prometheus_format() {
        let app = build_app(app_state());
        let req = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .header(AUTHORIZATION, "Bearer test-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(body.contains("# HELP"));
        assert!(body.contains("# TYPE"));
        assert!(body.contains("crossql_pipeline_duration_seconds"));
        assert!(body.contains("crossql_chunks_processed_total"));
        assert!(body.contains("crossql_active_pipelines"));
        assert!(body.contains("crossql_memory_bytes_used"));
    }

    #[tokio::test]
    async fn test_active_pipelines_gauge_increments_on_job_start() {
        let metrics = MetricsRegistry::new();
        metrics.job_started("orders_join");
        let body = metrics.render_prometheus().await;
        assert!(body.contains("crossql_active_pipelines 1"));
    }

    #[tokio::test]
    async fn test_active_pipelines_gauge_decrements_on_job_complete() {
        let metrics = MetricsRegistry::new();
        metrics.job_started("orders_join");
        metrics.job_finished("orders_join", Some(1.2)).await;
        let body = metrics.render_prometheus().await;
        assert!(body.contains("crossql_active_pipelines 0"));
    }

    #[tokio::test]
    async fn test_chunks_processed_counter_increments() {
        let metrics = MetricsRegistry::new();
        metrics.increment_chunks_processed("postgres", 3).await;
        let body = metrics.render_prometheus().await;
        assert!(body.contains("crossql_chunks_processed_total{source=\"postgres\"} 3"));
    }

    #[tokio::test]
    async fn test_pipeline_duration_recorded_on_completion() {
        let metrics = MetricsRegistry::new();
        metrics.job_started("orders_join");
        metrics.job_finished("orders_join", Some(3.4)).await;
        let body = metrics.render_prometheus().await;
        assert!(body.contains("crossql_pipeline_duration_seconds_sum{pipeline=\"orders_join\"} 3.4"));
        assert!(body.contains("crossql_pipeline_duration_seconds_count{pipeline=\"orders_join\"} 1"));
    }
}
