use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PipelineDefinition {
    pub pipeline_id: String,
    pub pipeline_version: u32,
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    pub sources: HashMap<String, SourceConfig>,
    pub execution: ExecutionConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PipelineNode {
    pub id: String,
    pub node_type: String,
    pub config: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PipelineEdge {
    pub source: String,
    pub target: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SourceConfig {
    pub alias: String,
    pub source_type: SourceType,
    pub partition_key: Option<String>,
    pub partition_hint: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SourceType {
    PostgreSQL,
    MySQL,
    MongoDB,
    Cassandra,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExecutionConfig {
    pub chunk_size: u64,
    pub max_concurrency: usize,
    pub streaming: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            chunk_size: 50_000,
            max_concurrency: 4,
            streaming: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JobRecord {
    pub job_id: String,
    pub pipeline_id: String,
    pub pipeline_version: u32,
    pub status: JobStatus,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_secs: Option<f64>,
    pub output_path: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NodeProgress {
    pub node_id: String,
    pub status: NodeStatus,
    pub rows_processed: Option<u64>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Idle,
    Running,
    Done,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_definition_serialization() {
        let mut sources = HashMap::new();
        sources.insert(
            "src_orders".to_string(),
            SourceConfig {
                alias: "POSTGRES".to_string(),
                source_type: SourceType::PostgreSQL,
                partition_key: Some("id".to_string()),
                partition_hint: Some(100_000),
            },
        );
        let pipeline = PipelineDefinition {
            pipeline_id: "orders_join".to_string(),
            pipeline_version: 3,
            nodes: vec![PipelineNode {
                id: "n1".to_string(),
                node_type: "postgres_source".to_string(),
                config: serde_json::json!({ "query": "SELECT 1" }),
            }],
            edges: vec![PipelineEdge {
                source: "n1".to_string(),
                target: "n2".to_string(),
            }],
            sources,
            execution: ExecutionConfig::default(),
        };

        let json = serde_json::to_string(&pipeline).unwrap();
        let decoded: PipelineDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, pipeline);
    }

    #[test]
    fn test_source_config_no_credentials() {
        let source = SourceConfig {
            alias: "MYSQL".to_string(),
            source_type: SourceType::MySQL,
            partition_key: None,
            partition_hint: None,
        };

        let value = serde_json::to_value(source).unwrap();
        assert!(value.get("alias").is_some());
        assert!(value.get("source_type").is_some());
        assert!(value.get("password").is_none());
        assert!(value.get("user").is_none());
        assert!(value.get("token").is_none());
    }

    #[test]
    fn test_execution_config_defaults() {
        let cfg = ExecutionConfig::default();
        assert_eq!(cfg.chunk_size, 50_000);
        assert_eq!(cfg.max_concurrency, 4);
        assert!(cfg.streaming);
    }

    #[test]
    fn test_job_status_all_variants() {
        let statuses = [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: JobStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn test_node_status_all_variants() {
        let statuses = [
            NodeStatus::Idle,
            NodeStatus::Running,
            NodeStatus::Done,
            NodeStatus::Failed,
        ];

        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: NodeStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn test_job_record_optional_fields() {
        let record = JobRecord {
            job_id: "job-1".to_string(),
            pipeline_id: "orders_join".to_string(),
            pipeline_version: 1,
            status: JobStatus::Queued,
            submitted_at: Utc::now(),
            started_at: None,
            completed_at: None,
            duration_secs: None,
            output_path: None,
            error: None,
        };

        let value = serde_json::to_value(record).unwrap();
        assert!(value.get("started_at").unwrap().is_null());
        assert!(value.get("completed_at").unwrap().is_null());
        assert!(value.get("duration_secs").unwrap().is_null());
        assert!(value.get("output_path").unwrap().is_null());
        assert!(value.get("error").unwrap().is_null());
    }
}
