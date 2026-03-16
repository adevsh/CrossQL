use crate::connector::ConnectorPool;
use crate::executor::chunk::schedule_chunks;
use crate::executor::dag::resolve_execution_levels;
use crate::executor::sink::{sink_parquet, sink_parquet_stream, SinkCompression};
use crate::server::metrics::MetricsRegistry;
use crate::storage::jobs::{JobQueue, JobQueueError};
use chrono::Utc;
use crossql_shared::{NodeStatus, PipelineDefinition, PipelineNode, SourceConfig, SourceType};
use polars::lazy::prelude::IntoLazy;
use polars::prelude::{Column as PolarsColumn, DataFrame, NamedFrom, Series};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

pub struct ExecutionOutcome {
    pub output_path: String,
}

#[derive(Deserialize)]
struct JoinConfig {
    how: Option<String>,
}

pub async fn execute_pipeline(
    jobs: JobQueue,
    metrics: MetricsRegistry,
    definition: PipelineDefinition,
    output_root: PathBuf,
    job_id: String,
    cancel_token: CancellationToken,
) -> Result<ExecutionOutcome, String> {
    let connectors = ConnectorPool::build(&definition)
        .await
        .map_err(|e| format!("Connector setup failed: {:?}", e))?;
    let plan = resolve_execution_levels(&definition)?;
    let node_map: HashMap<String, PipelineNode> = definition
        .nodes
        .iter()
        .map(|node| (node.id.clone(), node.clone()))
        .collect();
    let mut node_rows: HashMap<String, u64> = HashMap::new();

    let mut result_rows: Vec<(String, u64)> = Vec::new();

    for level in plan.levels.iter() {
        ensure_not_cancelled(&cancel_token)?;
        for node_id in level {
            ensure_not_cancelled(&cancel_token)?;
            jobs.set_node_status(&job_id, node_id, NodeStatus::Running, None)
                .await
                .map_err(job_error)?;
            let node = node_map
                .get(node_id)
                .ok_or_else(|| format!("Node not found: {}", node_id))?;
            let rows = evaluate_node_rows(
                &definition,
                node,
                &connectors,
                &node_rows,
                &cancel_token,
                &metrics,
            )
            .await?;
            jobs.set_node_status(&job_id, node_id, NodeStatus::Done, Some(rows))
                .await
                .map_err(job_error)?;
            node_rows.insert(node_id.clone(), rows);
            result_rows.push((node_id.clone(), rows));
        }
    }

    let total_rows = result_rows.iter().map(|(_, rows)| *rows).sum::<u64>();
    metrics.set_memory_bytes_used(total_rows.saturating_mul(128));

    let frame = build_result_frame(&result_rows)?;
    let output_path = if definition.execution.streaming {
        sink_parquet_stream(
            frame,
            &output_root,
            &definition.pipeline_id,
            definition.pipeline_version,
            &job_id,
            Utc::now(),
            SinkCompression::Snappy,
        )?
    } else {
        sink_parquet(
            frame,
            &output_root,
            &definition.pipeline_id,
            definition.pipeline_version,
            &job_id,
            Utc::now(),
            SinkCompression::Snappy,
        )?
    };

    Ok(ExecutionOutcome {
        output_path: output_path.to_string_lossy().to_string(),
    })
}

async fn evaluate_node_rows(
    definition: &PipelineDefinition,
    node: &PipelineNode,
    connectors: &ConnectorPool,
    node_rows: &HashMap<String, u64>,
    cancel_token: &CancellationToken,
    metrics: &MetricsRegistry,
) -> Result<u64, String> {
    if let Some((source_key, source)) = source_for_node(definition, node) {
        let query = source_query(node)?;
        let row_count = source_row_count(connectors, source, &query).await?;

        if definition.execution.streaming {
            let chunks = schedule_chunks(source, &definition.execution);
            for _ in chunks {
                ensure_not_cancelled(cancel_token)?;
                metrics
                    .increment_chunks_processed(source_key.as_str(), 1)
                    .await;
                tokio::task::yield_now().await;
            }
        } else {
            metrics
                .increment_chunks_processed(source_key.as_str(), 1)
                .await;
        }

        ensure_not_cancelled(cancel_token)?;
        return Ok(row_count.max(1));
    }

    let incoming = incoming_nodes(definition, &node.id);
    if node.node_type == "join" {
        return join_result_rows(node, &incoming, node_rows);
    }
    if let Some(first) = incoming.first() {
        return Ok(node_rows.get(first).copied().unwrap_or(1));
    }
    Ok(node_rows.get(&node.id).copied().unwrap_or(1))
}

fn source_query(node: &PipelineNode) -> Result<String, String> {
    let query = node
        .config
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    if query.is_empty() {
        return Err(format!("Source query is required for node {}", node.id));
    }
    Ok(query)
}

async fn source_row_count(
    connectors: &ConnectorPool,
    source: &SourceConfig,
    query: &str,
) -> Result<u64, String> {
    let wrapped = format!("SELECT COUNT(*) AS cnt FROM ({}) AS crossql_src", query);
    match source.source_type {
        SourceType::PostgreSQL => {
            let pool = connectors
                .postgres
                .get(&source.alias)
                .ok_or_else(|| format!("Missing PostgreSQL connector for {}", source.alias))?;
            let count: i64 = sqlx::query_scalar(&wrapped)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("PostgreSQL query failed for {}: {}", source.alias, e))?;
            Ok(count.max(0) as u64)
        }
        SourceType::MySQL => {
            let pool = connectors
                .mysql
                .get(&source.alias)
                .ok_or_else(|| format!("Missing MySQL connector for {}", source.alias))?;
            let count: i64 = sqlx::query_scalar(&wrapped)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("MySQL query failed for {}: {}", source.alias, e))?;
            Ok(count.max(0) as u64)
        }
        SourceType::MongoDB => Err(format!(
            "MongoDB execution is not implemented yet for source {}",
            source.alias
        )),
        SourceType::Cassandra => Err(format!(
            "Cassandra execution is not implemented yet for source {}",
            source.alias
        )),
    }
}

fn incoming_nodes(definition: &PipelineDefinition, node_id: &str) -> Vec<String> {
    definition
        .edges
        .iter()
        .filter(|e| e.target == node_id)
        .map(|e| e.source.clone())
        .collect()
}

fn join_result_rows(
    node: &PipelineNode,
    incoming: &[String],
    node_rows: &HashMap<String, u64>,
) -> Result<u64, String> {
    if incoming.len() < 2 {
        return Err(format!("Join node {} requires two inputs", node.id));
    }
    let left_rows = node_rows.get(&incoming[0]).copied().unwrap_or(0);
    let right_rows = node_rows.get(&incoming[1]).copied().unwrap_or(0);
    let cfg: JoinConfig = serde_json::from_value(node.config.clone())
        .map_err(|e| format!("Invalid join config for {}: {}", node.id, e))?;
    let how = cfg
        .how
        .unwrap_or_else(|| "inner".to_string())
        .to_ascii_lowercase();
    let out = match how.as_str() {
        "inner" => left_rows.min(right_rows),
        "left" => left_rows,
        "right" => right_rows,
        other => return Err(format!("Unsupported join type for {}: {}", node.id, other)),
    };
    Ok(out.max(1))
}

fn source_for_node<'a>(
    definition: &'a PipelineDefinition,
    node: &'a PipelineNode,
) -> Option<(String, &'a SourceConfig)> {
    let config_alias = node
        .config
        .get("source_alias")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let derived_alias = node.id.to_uppercase();

    definition
        .sources
        .iter()
        .find(|(key, source)| {
            let key_matches = *key == &node.id || *key == &derived_alias;
            let alias_matches = source.alias == derived_alias
                || config_alias
                    .as_ref()
                    .map(|alias| source.alias == *alias || *key == alias)
                    .unwrap_or(false);
            key_matches || alias_matches
        })
        .map(|(key, source)| {
            let label = if source.alias.is_empty() {
                key.to_lowercase()
            } else {
                source.alias.to_lowercase()
            };
            (label, source)
        })
}

fn build_result_frame(rows: &[(String, u64)]) -> Result<polars::lazy::prelude::LazyFrame, String> {
    if rows.is_empty() {
        let columns: Vec<PolarsColumn> = vec![
            Series::new("node_id".into(), &["none"]).into(),
            Series::new("rows".into(), &[0_u64]).into(),
        ];
        let df = DataFrame::new(1, columns)
            .map_err(|e| format!("Failed to build empty result frame: {}", e))?;
        return Ok(df.lazy());
    }

    let node_ids: Vec<String> = rows.iter().map(|(node_id, _)| node_id.clone()).collect();
    let processed_rows: Vec<u64> = rows.iter().map(|(_, rows)| *rows).collect();
    let columns: Vec<PolarsColumn> = vec![
        Series::new("node_id".into(), node_ids).into(),
        Series::new("rows".into(), processed_rows).into(),
    ];
    let df = DataFrame::new(rows.len(), columns)
        .map_err(|e| format!("Failed to build result frame: {}", e))?;
    Ok(df.lazy())
}

fn ensure_not_cancelled(cancel_token: &CancellationToken) -> Result<(), String> {
    if cancel_token.is_cancelled() {
        return Err("Cancelled".to_string());
    }
    Ok(())
}

fn job_error(err: JobQueueError) -> String {
    format!("{:?}", err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::metrics::MetricsRegistry;
    use crossql_shared::{ExecutionConfig, PipelineEdge, SourceType};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn temp_output() -> PathBuf {
        std::env::temp_dir().join(format!("crossql-engine-exec-test-{}", Uuid::new_v4()))
    }

    fn pipeline(streaming: bool) -> PipelineDefinition {
        PipelineDefinition {
            pipeline_id: "p_exec".to_string(),
            pipeline_version: 1,
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
            edges: vec![PipelineEdge {
                source: "n1".to_string(),
                target: "n2".to_string(),
            }],
            sources: HashMap::new(),
            execution: ExecutionConfig {
                chunk_size: 50_000,
                max_concurrency: 2,
                streaming,
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_pipeline_streaming_writes_output() {
        let root = temp_output();
        let jobs = JobQueue::new(&root).unwrap();
        let definition = pipeline(true);
        jobs.enqueue("job1".to_string(), &definition).await.unwrap();
        jobs.start("job1").await.unwrap();
        let outcome = execute_pipeline(
            jobs.clone(),
            MetricsRegistry::new(),
            definition,
            root.clone(),
            "job1".to_string(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(std::path::Path::new(&outcome.output_path).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_execute_pipeline_batch_writes_output() {
        let root = temp_output();
        let jobs = JobQueue::new(&root).unwrap();
        let definition = pipeline(false);
        jobs.enqueue("job2".to_string(), &definition).await.unwrap();
        jobs.start("job2").await.unwrap();
        let outcome = execute_pipeline(
            jobs.clone(),
            MetricsRegistry::new(),
            definition,
            root.clone(),
            "job2".to_string(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(std::path::Path::new(&outcome.output_path).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_source_for_node_match_by_config_alias() {
        let mut sources = HashMap::new();
        sources.insert(
            "src_orders".to_string(),
            SourceConfig {
                alias: "POSTGRES".to_string(),
                source_type: SourceType::PostgreSQL,
                partition_key: None,
                partition_hint: None,
            },
        );
        let definition = PipelineDefinition {
            pipeline_id: "p".to_string(),
            pipeline_version: 1,
            nodes: vec![],
            edges: vec![],
            sources,
            execution: ExecutionConfig::default(),
        };
        let node = PipelineNode {
            id: "n1".to_string(),
            node_type: "source".to_string(),
            config: serde_json::json!({ "source_alias": "src_orders" }),
        };
        assert!(source_for_node(&definition, &node).is_some());
    }
}
