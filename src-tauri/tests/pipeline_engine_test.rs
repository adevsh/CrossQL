use crossql_lib::engine::pipeline::{
    noop_progress, FlowEdge, FlowNode, FlowNodeData, PipelineEngine,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

fn node(id: &str, node_type: &str, config: serde_json::Value) -> FlowNode {
    FlowNode {
        id: id.to_string(),
        node_type: node_type.to_string(),
        data: Some(FlowNodeData {
            config,
            stats: None,
        }),
    }
}

fn edge(id: &str, source: &str, target: &str) -> FlowEdge {
    FlowEdge {
        id: id.to_string(),
        source: source.to_string(),
        target: target.to_string(),
        source_handle: None,
        target_handle: None,
    }
}

#[doc = "Expected: a pre-cancelled token aborts immediately.\nBehavior: run_with_cancel returns Err(\"Cancelled\") before graph validation or execution."]
#[tokio::test]
async fn run_with_cancel_returns_cancelled_when_token_pre_cancelled() {
    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = PipelineEngine::run_with_cancel(
        vec![],
        vec![],
        false,
        noop_progress(),
        cancel,
    )
    .await;

    match result {
        Err(e) => assert_eq!(e, "Cancelled"),
        Ok(_) => panic!("expected cancellation error"),
    }
}

#[doc = "Expected: pipeline must have exactly one parquet output node.\nBehavior: run_with_cancel fails when there is no parquet output node."]
#[tokio::test]
async fn run_rejects_when_output_node_count_is_zero() {
    let nodes = vec![node("n1", "filter", json!({}))];
    let result = PipelineEngine::run_with_cancel(
        nodes,
        vec![],
        false,
        noop_progress(),
        CancellationToken::new(),
    )
    .await;

    match result {
        Err(e) => assert_eq!(e, "Exactly one Parquet Output node is required"),
        Ok(_) => panic!("expected output node count validation error"),
    }
}

#[doc = "Expected: pipeline must have exactly one parquet output node.\nBehavior: run_with_cancel fails when there are multiple parquet output nodes."]
#[tokio::test]
async fn run_rejects_when_output_node_count_is_multiple() {
    let nodes = vec![
        node("out1", "parquet", json!({"path": "/tmp/a.parquet", "compression": "snappy"})),
        node("out2", "parquet", json!({"path": "/tmp/b.parquet", "compression": "snappy"})),
    ];
    let result = PipelineEngine::run_with_cancel(
        nodes,
        vec![],
        false,
        noop_progress(),
        CancellationToken::new(),
    )
    .await;

    match result {
        Err(e) => assert_eq!(e, "Exactly one Parquet Output node is required"),
        Ok(_) => panic!("expected output node count validation error"),
    }
}

#[doc = "Expected: parquet output node has exactly one incoming edge.\nBehavior: run_with_cancel fails when output node has more than one incoming edge."]
#[tokio::test]
async fn run_rejects_when_output_node_incoming_edge_count_is_not_one() {
    let nodes = vec![
        node("src1", "filter", json!({})),
        node("src2", "select", json!({})),
        node("out1", "parquet", json!({"path": "/tmp/a.parquet", "compression": "snappy"})),
    ];
    let edges = vec![
        edge("e1", "src1", "out1"),
        edge("e2", "src2", "out1"),
    ];

    let result = PipelineEngine::run_with_cancel(
        nodes,
        edges,
        false,
        noop_progress(),
        CancellationToken::new(),
    )
    .await;

    match result {
        Err(e) => assert_eq!(e, "Output node must have exactly one incoming edge"),
        Ok(_) => panic!("expected output edge count validation error"),
    }
}
