use chrono::Utc;
use crossql_shared::{JobRecord, JobStatus, NodeProgress, NodeStatus, PipelineDefinition};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct JobQueue {
    runs_path: Arc<PathBuf>,
    inner: Arc<RwLock<HashMap<String, StoredJob>>>,
}

#[derive(Clone)]
struct StoredJob {
    record: JobRecord,
    node_progress: Vec<NodeProgress>,
}

#[derive(Debug)]
pub enum JobQueueError {
    NotFound(String),
    InvalidTransition {
        job_id: String,
        from: JobStatus,
        to: JobStatus,
    },
    Io(String),
}

impl JobQueue {
    pub fn new(output_root: &Path) -> io::Result<Self> {
        fs::create_dir_all(output_root)?;
        let runs_path = output_root.join("runs.json");
        let queue = Self {
            runs_path: Arc::new(runs_path),
            inner: Arc::new(RwLock::new(HashMap::new())),
        };
        queue.load_from_disk()?;
        Ok(queue)
    }

    pub fn runs_path(&self) -> PathBuf {
        self.runs_path.as_ref().clone()
    }

    pub async fn enqueue(&self, job_id: String, pipeline: &PipelineDefinition) -> Result<JobRecord, JobQueueError> {
        let submitted_at = Utc::now();
        let record = JobRecord {
            job_id: job_id.clone(),
            pipeline_id: pipeline.pipeline_id.clone(),
            pipeline_version: pipeline.pipeline_version,
            status: JobStatus::Queued,
            submitted_at,
            started_at: None,
            completed_at: None,
            duration_secs: None,
            output_path: None,
            error: None,
        };
        let progress = pipeline
            .nodes
            .iter()
            .map(|n| NodeProgress {
                node_id: n.id.clone(),
                status: NodeStatus::Idle,
                rows_processed: None,
                started_at: None,
                completed_at: None,
            })
            .collect();

        let mut guard = self.inner.write().await;
        guard.insert(
            job_id,
            StoredJob {
                record: record.clone(),
                node_progress: progress,
            },
        );
        self.persist_locked(&guard)?;
        Ok(record)
    }

    pub async fn start(&self, job_id: &str) -> Result<JobRecord, JobQueueError> {
        self.transition_status(job_id, JobStatus::Queued, JobStatus::Running, None, None)
            .await
    }

    pub async fn complete(&self, job_id: &str, output_path: Option<String>) -> Result<JobRecord, JobQueueError> {
        self.transition_status(job_id, JobStatus::Running, JobStatus::Completed, output_path, None)
            .await
    }

    pub async fn fail(&self, job_id: &str, message: String) -> Result<JobRecord, JobQueueError> {
        self.transition_status(job_id, JobStatus::Running, JobStatus::Failed, None, Some(message))
            .await
    }

    pub async fn cancel(&self, job_id: &str) -> Result<JobRecord, JobQueueError> {
        let mut guard = self.inner.write().await;
        let record = {
            let stored = guard
                .get_mut(job_id)
                .ok_or_else(|| JobQueueError::NotFound(job_id.to_string()))?;
            match stored.record.status {
            JobStatus::Queued | JobStatus::Running => {
                stored.record.status = JobStatus::Cancelled;
                stored.record.completed_at = Some(Utc::now());
                if let Some(started_at) = stored.record.started_at {
                    stored.record.duration_secs =
                        Some((Utc::now() - started_at).num_milliseconds() as f64 / 1000.0);
                }
                stored.record.clone()
            }
            _ => return Err(JobQueueError::InvalidTransition {
                job_id: job_id.to_string(),
                from: stored.record.status.clone(),
                to: JobStatus::Cancelled,
            }),
            }
        };
        self.persist_locked(&guard)?;
        Ok(record)
    }

    pub async fn get(&self, job_id: &str) -> Option<(JobRecord, Vec<NodeProgress>)> {
        let guard = self.inner.read().await;
        guard.get(job_id).map(|j| (j.record.clone(), j.node_progress.clone()))
    }

    pub async fn list(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        let mut out: Vec<JobRecord> = guard.values().map(|j| j.record.clone()).collect();
        out.sort_by(|a, b| b.submitted_at.cmp(&a.submitted_at));
        out
    }

    pub async fn set_node_status(
        &self,
        job_id: &str,
        node_id: &str,
        status: NodeStatus,
        rows_processed: Option<u64>,
    ) -> Result<(), JobQueueError> {
        let mut guard = self.inner.write().await;
        let stored = guard
            .get_mut(job_id)
            .ok_or_else(|| JobQueueError::NotFound(job_id.to_string()))?;
        if matches!(
            stored.record.status,
            JobStatus::Cancelled | JobStatus::Completed | JobStatus::Failed
        ) {
            return Ok(());
        }
        if let Some(node) = stored.node_progress.iter_mut().find(|n| n.node_id == node_id) {
            match status {
                NodeStatus::Running => node.started_at = Some(Utc::now()),
                NodeStatus::Done | NodeStatus::Failed => node.completed_at = Some(Utc::now()),
                NodeStatus::Idle => {}
            }
            node.status = status;
            node.rows_processed = rows_processed;
        }
        self.persist_locked(&guard)?;
        Ok(())
    }

    pub async fn counts(&self) -> (u32, u32, u32) {
        let guard = self.inner.read().await;
        let mut active = 0_u32;
        let mut queued = 0_u32;
        let mut completed = 0_u32;
        for j in guard.values() {
            match j.record.status {
                JobStatus::Queued => queued += 1,
                JobStatus::Running => active += 1,
                JobStatus::Completed => completed += 1,
                JobStatus::Failed | JobStatus::Cancelled => {}
            }
        }
        (active, queued, completed)
    }

    async fn transition_status(
        &self,
        job_id: &str,
        expected_from: JobStatus,
        next: JobStatus,
        output_path: Option<String>,
        error: Option<String>,
    ) -> Result<JobRecord, JobQueueError> {
        let mut guard = self.inner.write().await;
        let record = {
            let stored = guard
                .get_mut(job_id)
                .ok_or_else(|| JobQueueError::NotFound(job_id.to_string()))?;

            if stored.record.status != expected_from {
                return Err(JobQueueError::InvalidTransition {
                    job_id: job_id.to_string(),
                    from: stored.record.status.clone(),
                    to: next,
                });
            }

            let now = Utc::now();
            stored.record.status = next;
            if stored.record.status == JobStatus::Running {
                stored.record.started_at = Some(now);
            } else {
                stored.record.completed_at = Some(now);
                if let Some(started_at) = stored.record.started_at {
                    stored.record.duration_secs =
                        Some((now - started_at).num_milliseconds() as f64 / 1000.0);
                }
                stored.record.output_path = output_path;
                stored.record.error = error;
            }
            stored.record.clone()
        };
        self.persist_locked(&guard)?;
        Ok(record)
    }

    fn persist_locked(&self, jobs: &HashMap<String, StoredJob>) -> Result<(), JobQueueError> {
        let mut records: Vec<JobRecord> = jobs.values().map(|j| j.record.clone()).collect();
        records.sort_by(|a, b| b.submitted_at.cmp(&a.submitted_at));
        let body = serde_json::to_string_pretty(&records)
            .map_err(|e| JobQueueError::Io(format!("Failed to serialize runs.json: {}", e)))?;
        fs::write(self.runs_path.as_ref(), body)
            .map_err(|e| JobQueueError::Io(format!("Failed to write runs.json: {}", e)))
    }

    fn load_from_disk(&self) -> io::Result<()> {
        let path = self.runs_path.as_ref();
        if !path.exists() {
            return Ok(());
        }
        let body = fs::read_to_string(path)?;
        if body.trim().is_empty() {
            return Ok(());
        }
        let records: Vec<JobRecord> = serde_json::from_str(&body).unwrap_or_default();
        let mut map = HashMap::new();
        for record in records {
            map.insert(
                record.job_id.clone(),
                StoredJob {
                    record,
                    node_progress: vec![],
                },
            );
        }
        let mut guard = self
            .inner
            .try_write()
            .map_err(|_| io::Error::other("Failed to acquire startup write lock"))?;
        *guard = map;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossql_shared::{ExecutionConfig, PipelineEdge, PipelineNode, SourceConfig, SourceType};
    use uuid::Uuid;

    fn temp_output() -> PathBuf {
        std::env::temp_dir().join(format!("crossql-jobs-{}", Uuid::new_v4()))
    }

    fn pipeline() -> PipelineDefinition {
        let mut sources = HashMap::new();
        sources.insert(
            "s1".to_string(),
            SourceConfig {
                alias: "POSTGRES".to_string(),
                source_type: SourceType::PostgreSQL,
                partition_key: None,
                partition_hint: None,
            },
        );
        PipelineDefinition {
            pipeline_id: "pipeline_1".to_string(),
            pipeline_version: 1,
            nodes: vec![PipelineNode {
                id: "n1".to_string(),
                node_type: "source".to_string(),
                config: serde_json::json!({}),
            }],
            edges: vec![PipelineEdge {
                source: "n1".to_string(),
                target: "n2".to_string(),
            }],
            sources,
            execution: ExecutionConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_enqueue_sets_queued_status() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        let record = queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        assert_eq!(record.status, JobStatus::Queued);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_start_transitions_queued_to_running() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        let record = queue.start("job1").await.unwrap();
        assert_eq!(record.status, JobStatus::Running);
        assert!(record.started_at.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_complete_transitions_running_to_completed() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        queue.start("job1").await.unwrap();
        let record = queue.complete("job1", None).await.unwrap();
        assert_eq!(record.status, JobStatus::Completed);
        assert!(record.completed_at.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_fail_transitions_running_to_failed() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        queue.start("job1").await.unwrap();
        let record = queue.fail("job1", "boom".to_string()).await.unwrap();
        assert_eq!(record.status, JobStatus::Failed);
        assert_eq!(record.error.as_deref(), Some("boom"));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_cancel_transitions_running_to_cancelled() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        queue.start("job1").await.unwrap();
        let record = queue.cancel("job1").await.unwrap();
        assert_eq!(record.status, JobStatus::Cancelled);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_invalid_transition_returns_error() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        let result = queue.complete("job1", None).await;
        assert!(result.is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_list_returns_all_jobs_sorted_by_submitted_at() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        queue.enqueue("job2".to_string(), &pipeline()).await.unwrap();

        let jobs = queue.list().await;
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, "job2");
        assert_eq!(jobs[1].job_id, "job1");
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_set_node_status_ignored_after_cancel() {
        let root = temp_output();
        let queue = JobQueue::new(&root).unwrap();
        queue.enqueue("job1".to_string(), &pipeline()).await.unwrap();
        queue.start("job1").await.unwrap();
        queue
            .set_node_status("job1", "n1", NodeStatus::Running, Some(10))
            .await
            .unwrap();
        queue.cancel("job1").await.unwrap();
        queue
            .set_node_status("job1", "n1", NodeStatus::Done, Some(99))
            .await
            .unwrap();
        let (_, progress) = queue.get("job1").await.unwrap();
        let n1 = progress.into_iter().find(|p| p.node_id == "n1").unwrap();
        assert_eq!(n1.status, NodeStatus::Running);
        assert_eq!(n1.rows_processed, Some(10));
        let _ = fs::remove_dir_all(root);
    }
}
