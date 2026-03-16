use chrono::Utc;
use crossql_shared::{NodeProgress, NodeStatus};
use polars::lazy::prelude::LazyFrame;
use std::collections::HashMap;
use tokio::task::JoinSet;

pub async fn run_independent_nodes(
    frames: Vec<(String, LazyFrame)>,
) -> Result<(HashMap<String, LazyFrame>, Vec<NodeProgress>), String> {
    let mut progress: HashMap<String, NodeProgress> = HashMap::new();
    let mut set = JoinSet::new();
    let mut total = frames.len();

    for (node_id, frame) in frames {
        progress.insert(
            node_id.clone(),
            NodeProgress {
                node_id: node_id.clone(),
                status: NodeStatus::Running,
                rows_processed: None,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        );
        set.spawn(async move {
            let rows = frame
                .clone()
                .collect()
                .map_err(|e| format!("Node {} failed: {}", node_id, e))?
                .height() as u64;
            Ok::<(String, LazyFrame, u64), String>((node_id, frame, rows))
        });
    }

    let mut outputs = HashMap::new();

    while total > 0 {
        let joined = set
            .join_next()
            .await
            .ok_or_else(|| "JoinSet ended unexpectedly".to_string())?;
        total -= 1;

        match joined {
            Ok(Ok((node_id, frame, rows))) => {
                if let Some(p) = progress.get_mut(&node_id) {
                    p.status = NodeStatus::Done;
                    p.rows_processed = Some(rows);
                    p.completed_at = Some(Utc::now());
                }
                outputs.insert(node_id, frame);
            }
            Ok(Err(err)) => {
                return Err(err);
            }
            Err(err) => {
                return Err(format!("Node task join error: {}", err));
            }
        }
    }

    Ok((outputs, progress.into_values().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::lazy::prelude::IntoLazy;
    use polars::prelude::{DataFrame, NamedFrom, Series};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_run_independent_nodes_runs_in_parallel_set() {
        let f1 = DataFrame::new(2, vec![Series::new("id".into(), &[1_i64, 2]).into()])
            .unwrap()
            .lazy();
        let f2 = DataFrame::new(3, vec![Series::new("id".into(), &[3_i64, 4, 5]).into()])
            .unwrap()
            .lazy();

        let (out, progress) = run_independent_nodes(vec![
            ("a".to_string(), f1),
            ("b".to_string(), f2),
        ])
        .await
        .unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(progress.len(), 2);
        assert!(progress.iter().all(|p| p.status == NodeStatus::Done));
        assert!(progress.iter().all(|p| p.rows_processed.unwrap_or_default() > 0));
    }
}
