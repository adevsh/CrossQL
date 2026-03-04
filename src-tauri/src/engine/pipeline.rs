use crate::connectors::cassandra::CassandraConnector;
use crate::connectors::mongodb::MongoConnector;
use crate::connectors::mysql::MysqlConnector;
use crate::connectors::postgres::PostgresConnector;
use crate::engine::schema::{apply_schema_maps, validate_no_nulls, SchemaMapConfig};
use crate::engine::transform::{
    CastConfig, DerivedColumnConfig, FilterConfig, RenameConfig, SelectConfig, Transformer,
};
use crate::writer::parquet::CrossQLParquetWriter;
use polars::lazy::prelude::*;
use polars::prelude::IntoLazy;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;

/// Callback for per-node progress: (node_id, state) where state is "running" or "done".
pub type NodeProgressFn = Arc<dyn Fn(&str, &str) + Send + Sync>;

pub fn noop_progress() -> NodeProgressFn {
    Arc::new(|_node_id: &str, _state: &str| {})
}

#[derive(serde::Deserialize)]
pub struct FlowNodeData {
    pub config: serde_json::Value,
    #[serde(default)]
    pub stats: Option<serde_json::Value>,
}

#[derive(serde::Deserialize)]
pub struct FlowNode {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub data: Option<FlowNodeData>,
}

#[derive(serde::Deserialize)]
pub struct FlowEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(rename = "sourceHandle")]
    pub source_handle: Option<String>,
    #[serde(rename = "targetHandle")]
    pub target_handle: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct JoinConfig {
    pub left_on: Option<String>,
    pub right_on: Option<String>,
    pub how: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct PostgresSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub query: String,
}

#[derive(serde::Deserialize)]
pub struct MongoSourceConfig {
    pub uri: String,
    pub database: String,
    pub collection: String,
    pub filter: Option<String>,
    pub projection: Option<String>,
    pub flatten_depth: Option<usize>,
}

#[derive(serde::Deserialize)]
pub struct CassandraSourceConfig {
    pub contact_points: String,
    pub keyspace: String,
    pub query: String,
}

#[derive(serde::Deserialize)]
pub struct ParquetConfig {
    pub path: String,
    pub compression: String,
    pub row_group_size: Option<usize>,
}

#[derive(serde::Serialize)]
pub struct NodeStats {
    pub id: String,
    pub rows_left: Option<u64>,
    pub rows_right: Option<u64>,
    pub rows_out: Option<u64>,
}

#[derive(serde::Serialize)]
pub struct PipelineRunResult {
    pub row_count: usize,
    pub path: String,
    pub file_size_bytes: u64,
    pub node_stats: Vec<NodeStats>,
}

#[derive(serde::Serialize)]
pub struct PreviewResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

fn anyvalue_to_json(v: AnyValue) -> serde_json::Value {
    match v {
        AnyValue::Null => serde_json::Value::Null,
        AnyValue::Boolean(x) => serde_json::Value::Bool(x),
        AnyValue::Int64(x) => serde_json::Value::Number(x.into()),
        AnyValue::Int32(x) => serde_json::Value::Number((x as i64).into()),
        AnyValue::UInt64(x) => serde_json::Value::Number(serde_json::Number::from(x)),
        AnyValue::UInt32(x) => serde_json::Value::Number(serde_json::Number::from(x)),
        AnyValue::Float64(x) => serde_json::Number::from_f64(x)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AnyValue::Float32(x) => serde_json::Number::from_f64(x as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AnyValue::String(x) => serde_json::Value::String(x.to_string()),
        AnyValue::StringOwned(x) => serde_json::Value::String(x.to_string()),
        AnyValue::Datetime(v, tu, _) => {
            let ms = match tu {
                TimeUnit::Nanoseconds => v / 1_000_000,
                TimeUnit::Microseconds => v / 1_000,
                TimeUnit::Milliseconds => v,
            };
            serde_json::Value::Number(ms.into())
        }
        other => serde_json::Value::String(other.to_string()),
    }
}

fn df_preview(df: &DataFrame, max_rows: usize) -> Result<PreviewResult, String> {
    let cols: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let mut rows: Vec<serde_json::Value> = Vec::new();

    let n = std::cmp::min(df.height(), max_rows);
    for i in 0..n {
        let mut obj = serde_json::Map::new();
        for c in &cols {
            let s = df
                .column(c)
                .map_err(|e| format!("Failed to access column '{}': {}", c, e))?;
            let v = s.get(i).map_err(|e| format!("Failed to read value: {}", e))?;
            obj.insert(c.clone(), anyvalue_to_json(v));
        }
        rows.push(serde_json::Value::Object(obj));
    }

    Ok(PreviewResult { columns: cols, rows })
}

fn parse_join_type(how: &str) -> Result<JoinType, String> {
    match how {
        "inner" => Ok(JoinType::Inner),
        "left" => Ok(JoinType::Left),
        "outer" => Ok(JoinType::Full),
        _ => Err(format!("Unsupported join type: {}", how)),
    }
}

fn compute_row_count(lf: &LazyFrame) -> Result<u64, String> {
    let df = lf
        .clone()
        .select([len().alias("row_count")])
        .collect()
        .map_err(|e| format!("Failed to compute row count: {}", e))?;
    let s = df
        .column("row_count")
        .map_err(|e| format!("Failed to read row_count: {}", e))?;
    let v = s.get(0).map_err(|e| format!("Failed to read row_count: {}", e))?;
    match v {
        AnyValue::UInt32(x) => Ok(x as u64),
        AnyValue::UInt64(x) => Ok(x),
        AnyValue::Int64(x) => Ok(x.max(0) as u64),
        AnyValue::Int32(x) => Ok((x as i64).max(0) as u64),
        _ => Err("Unexpected row_count dtype".to_string()),
    }
}

fn incoming_edges<'a>(edges: &'a [FlowEdge], target: &str) -> Vec<&'a FlowEdge> {
    edges.iter().filter(|e| e.target == target).collect()
}

fn resolve_join_inputs(edges: &[FlowEdge], join_id: &str) -> Result<(String, String), String> {
    let inc = incoming_edges(edges, join_id);
    if inc.len() != 2 {
        return Err("Join node must have exactly two incoming edges".to_string());
    }

    let mut left: Option<String> = None;
    let mut right: Option<String> = None;
    for e in &inc {
        match e.target_handle.as_deref() {
            Some("left") => left = Some(e.source.clone()),
            Some("right") => right = Some(e.source.clone()),
            _ => {}
        }
    }
    if let (Some(l), Some(r)) = (left, right) {
        return Ok((l, r));
    }

    let mut sources: Vec<String> = inc.iter().map(|e| e.source.clone()).collect();
    sources.sort();
    Ok((sources[0].clone(), sources[1].clone()))
}

fn node_to_lazyframe<'a>(
    node_id: &'a str,
    nodes_by_id: &'a HashMap<String, FlowNode>,
    edges: &'a [FlowEdge],
    cache: &'a mut HashMap<String, LazyFrame>,
    visiting: &'a mut HashSet<String>,
    error_on_null_cols: &'a mut Vec<String>,
    on_progress: &'a NodeProgressFn,
) -> Pin<Box<dyn Future<Output = Result<LazyFrame, String>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(lf) = cache.get(node_id) {
            return Ok(lf.clone());
        }
        if visiting.contains(node_id) {
            return Err("Cycle detected in pipeline graph".to_string());
        }

        let node = nodes_by_id
            .get(node_id)
            .ok_or_else(|| format!("Node not found: {}", node_id))?;
        visiting.insert(node_id.to_string());
        on_progress(node_id, "running");

        let lf = match node.node_type.as_str() {
        "postgres" => {
            let cfg: PostgresSourceConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid postgres config: {}", e))?;
            let df = PostgresConnector::fetch_dataframe(
                &cfg.host,
                cfg.port,
                &cfg.user,
                &cfg.password,
                &cfg.database,
                &cfg.query,
            )
            .await?;
            df.lazy()
        }
        "mysql" => {
            let cfg: PostgresSourceConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid mysql config: {}", e))?;
            let df = MysqlConnector::fetch_dataframe(
                &cfg.host,
                cfg.port,
                &cfg.user,
                &cfg.password,
                &cfg.database,
                &cfg.query,
            )
            .await?;
            df.lazy()
        }
        "mongodb" => {
            let cfg: MongoSourceConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid mongodb config: {}", e))?;
            let df = MongoConnector::fetch_dataframe(
                &cfg.uri,
                &cfg.database,
                &cfg.collection,
                cfg.filter.as_deref().unwrap_or("{}"),
                cfg.projection.as_deref().unwrap_or("{}"),
                cfg.flatten_depth.unwrap_or(1),
            )
            .await?;
            df.lazy()
        }
        "cassandra" => {
            let cfg: CassandraSourceConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid cassandra config: {}", e))?;
            let df = CassandraConnector::fetch_dataframe(&cfg.contact_points, &cfg.keyspace, &cfg.query).await?;
            df.lazy()
        }
        "schema_map" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Schema Map must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;

            let cfg: SchemaMapConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid schema_map config: {}", e))?;
            let (lf, err_cols) = apply_schema_maps(upstream_lf, &[cfg])?;
            error_on_null_cols.extend(err_cols);
            lf
        }
        "filter" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Filter must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let cfg: FilterConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid filter config: {}", e))?;
            Transformer::apply_filter(upstream_lf, cfg)?
        }
        "select" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Select must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let cfg: SelectConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid select config: {}", e))?;
            Transformer::apply_select(upstream_lf, cfg)?
        }
        "rename" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Rename must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let cfg: RenameConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid rename config: {}", e))?;
            Transformer::apply_rename(upstream_lf, cfg)?
        }
        "cast" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Cast must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let cfg: CastConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid cast config: {}", e))?;
            Transformer::apply_cast(upstream_lf, cfg)?
        }
        "derived" => {
            let inc = incoming_edges(edges, &node.id);
            if inc.len() != 1 {
                return Err("Derived must have exactly one incoming edge".to_string());
            }
            let upstream = &inc[0].source;
            let upstream_lf = node_to_lazyframe(
                upstream,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let cfg: DerivedColumnConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid derived config: {}", e))?;
            Transformer::apply_derived(upstream_lf, cfg)?
        }
        "join" => {
            let cfg: JoinConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid join config: {}", e))?;
            let how = cfg.how.unwrap_or_else(|| "inner".to_string()).to_lowercase();
            let join_type = parse_join_type(&how)?;

            let (left_id, right_id) = resolve_join_inputs(edges, &node.id)?;
            let left_lf = node_to_lazyframe(
                &left_id,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;
            let right_lf = node_to_lazyframe(
                &right_id,
                nodes_by_id,
                edges,
                cache,
                visiting,
                error_on_null_cols,
                on_progress,
            )
            .await?;

            let left_on = cfg.left_on.unwrap_or_default();
            let right_on = cfg.right_on.unwrap_or_default();
            if left_on.trim().is_empty() || right_on.trim().is_empty() {
                return Err("Join requires left_on and right_on".to_string());
            }
            left_lf.join(
                right_lf,
                [col(left_on.trim())],
                [col(right_on.trim())],
                JoinArgs::new(join_type),
            )
        }
        _ => return Err(format!("Unsupported node type: {}", node.node_type)),
        };

        visiting.remove(node_id);
        cache.insert(node_id.to_string(), lf.clone());
        on_progress(node_id, "done");
        Ok(lf)
    })
}

pub struct PipelineEngine;

impl PipelineEngine {
    pub async fn run(
        nodes: Vec<FlowNode>,
        edges: Vec<FlowEdge>,
        compute_stats: bool,
        on_progress: NodeProgressFn,
    ) -> Result<PipelineRunResult, String> {
        let mut nodes_by_id: HashMap<String, FlowNode> = HashMap::new();
        for n in nodes {
            nodes_by_id.insert(n.id.clone(), n);
        }

        let outputs: Vec<&FlowNode> = nodes_by_id
            .values()
            .filter(|n| n.node_type == "parquet")
            .collect();
        if outputs.len() != 1 {
            return Err("Exactly one Parquet Output node is required".to_string());
        }
        let out_node = outputs[0];
        let out_cfg: ParquetConfig =
            serde_json::from_value(out_node.data.as_ref().ok_or("Missing node data")?.config.clone())
                .map_err(|e| format!("Invalid parquet config: {}", e))?;

        let inc = incoming_edges(&edges, &out_node.id);
        if inc.len() != 1 {
            return Err("Output node must have exactly one incoming edge".to_string());
        }
        let upstream_id = &inc[0].source;

        let mut cache: HashMap<String, LazyFrame> = HashMap::new();
        let mut visiting: HashSet<String> = HashSet::new();
        let mut error_on_null_cols: Vec<String> = Vec::new();

        on_progress(&out_node.id, "running");

        let lf = node_to_lazyframe(
            upstream_id,
            &nodes_by_id,
            &edges,
            &mut cache,
            &mut visiting,
            &mut error_on_null_cols,
            &on_progress,
        )
        .await?;

        let df = lf
            .collect()
            .map_err(|e| format!("Failed to execute pipeline: {}", e))?;
        validate_no_nulls(&df, &error_on_null_cols)?;

        let output_row_count = df.height();
        CrossQLParquetWriter::write_dataframe(
            df,
            &out_cfg.path,
            &out_cfg.compression,
            out_cfg.row_group_size,
        )?;

        let file_size_bytes = fs::metadata(&out_cfg.path)
            .map_err(|e| format!("Failed to stat output file: {}", e))?
            .len();

        on_progress(&out_node.id, "done");

        let node_stats: Vec<NodeStats> = if compute_stats {
            let mut node_stats: Vec<NodeStats> = Vec::new();
            for (id, lf) in cache.iter() {
                let node = nodes_by_id.get(id);
                if let Some(n) = node {
                    if n.node_type == "join" {
                        let (left_id, right_id) = resolve_join_inputs(&edges, id)?;
                        let rows_left = cache.get(&left_id).and_then(|x| compute_row_count(x).ok());
                        let rows_right = cache.get(&right_id).and_then(|x| compute_row_count(x).ok());
                        let rows_out = compute_row_count(lf).ok();
                        node_stats.push(NodeStats {
                            id: id.clone(),
                            rows_left,
                            rows_right,
                            rows_out,
                        });
                        continue;
                    }
                }
                let rows_out = compute_row_count(lf).ok();
                node_stats.push(NodeStats {
                    id: id.clone(),
                    rows_left: None,
                    rows_right: None,
                    rows_out,
                });
            }
            node_stats
        } else {
            Vec::new()
        };

        Ok(PipelineRunResult {
            row_count: output_row_count,
            path: out_cfg.path,
            file_size_bytes,
            node_stats,
        })
    }

    pub async fn preview_node(
        nodes: Vec<FlowNode>,
        edges: Vec<FlowEdge>,
        node_id: String,
    ) -> Result<PreviewResult, String> {
        let mut nodes_by_id: HashMap<String, FlowNode> = HashMap::new();
        for n in nodes {
            nodes_by_id.insert(n.id.clone(), n);
        }

        let mut cache: HashMap<String, LazyFrame> = HashMap::new();
        let mut visiting: HashSet<String> = HashSet::new();
        let mut error_on_null_cols: Vec<String> = Vec::new();
        let progress = noop_progress();

        let node = nodes_by_id
            .get(&node_id)
            .ok_or_else(|| format!("Node not found: {}", node_id))?;

        let lf = if node.node_type == "join" {
            let cfg: JoinConfig =
                serde_json::from_value(node.data.as_ref().ok_or("Missing node data")?.config.clone())
                    .map_err(|e| format!("Invalid join config: {}", e))?;
            let how = cfg.how.unwrap_or_else(|| "inner".to_string()).to_lowercase();
            let join_type = parse_join_type(&how)?;

            let (left_id, right_id) = resolve_join_inputs(&edges, &node_id)?;
            let left_lf = node_to_lazyframe(
                &left_id,
                &nodes_by_id,
                &edges,
                &mut cache,
                &mut visiting,
                &mut error_on_null_cols,
                &progress,
            )
            .await?
            .limit(200);
            let right_lf = node_to_lazyframe(
                &right_id,
                &nodes_by_id,
                &edges,
                &mut cache,
                &mut visiting,
                &mut error_on_null_cols,
                &progress,
            )
            .await?
            .limit(200);

            let left_on = cfg.left_on.unwrap_or_default();
            let right_on = cfg.right_on.unwrap_or_default();
            if left_on.trim().is_empty() || right_on.trim().is_empty() {
                return Err("Join requires left_on and right_on".to_string());
            }
            left_lf.join(
                right_lf,
                [col(left_on.trim())],
                [col(right_on.trim())],
                JoinArgs::new(join_type),
            )
        } else {
            node_to_lazyframe(
                &node_id,
                &nodes_by_id,
                &edges,
                &mut cache,
                &mut visiting,
                &mut error_on_null_cols,
                &progress,
            )
            .await?
        };

        let df = lf
            .limit(50)
            .collect()
            .map_err(|e| format!("Failed to execute preview: {}", e))?;
        df_preview(&df, 50)
    }
}
