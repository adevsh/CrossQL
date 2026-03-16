use crossql_shared::PipelineDefinition;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagPlan {
    pub levels: Vec<Vec<String>>,
}

pub fn resolve_execution_levels(definition: &PipelineDefinition) -> Result<DagPlan, String> {
    let node_ids: Vec<String> = definition.nodes.iter().map(|n| n.id.clone()).collect();
    let node_set: HashSet<String> = node_ids.iter().cloned().collect();

    let mut indegree: HashMap<String, usize> =
        node_ids.iter().map(|id| (id.clone(), 0_usize)).collect();
    let mut outgoing: HashMap<String, Vec<String>> =
        node_ids.iter().map(|id| (id.clone(), Vec::new())).collect();

    for edge in &definition.edges {
        if !node_set.contains(&edge.source) {
            return Err(format!("Unknown edge source node: {}", edge.source));
        }
        if !node_set.contains(&edge.target) {
            return Err(format!("Unknown edge target node: {}", edge.target));
        }
        outgoing
            .get_mut(&edge.source)
            .ok_or_else(|| "Missing source node in adjacency map".to_string())?
            .push(edge.target.clone());
        *indegree
            .get_mut(&edge.target)
            .ok_or_else(|| "Missing target node in indegree map".to_string())? += 1;
    }

    let mut queue: VecDeque<String> = indegree
        .iter()
        .filter_map(|(id, d)| if *d == 0 { Some(id.clone()) } else { None })
        .collect();
    let mut visited = 0_usize;
    let mut levels: Vec<Vec<String>> = Vec::new();

    while !queue.is_empty() {
        let level_size = queue.len();
        let mut level: Vec<String> = Vec::with_capacity(level_size);

        for _ in 0..level_size {
            let node = queue.pop_front().ok_or_else(|| "Queue unexpectedly empty".to_string())?;
            visited += 1;
            level.push(node.clone());

            if let Some(targets) = outgoing.get(&node) {
                for target in targets {
                    if let Some(d) = indegree.get_mut(target) {
                        *d -= 1;
                        if *d == 0 {
                            queue.push_back(target.clone());
                        }
                    }
                }
            }
        }

        levels.push(level);
    }

    if visited != definition.nodes.len() {
        return Err("Pipeline graph contains a cycle".to_string());
    }

    Ok(DagPlan { levels })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossql_shared::{
        ExecutionConfig, PipelineDefinition, PipelineEdge, PipelineNode, SourceConfig, SourceType,
    };
    use std::collections::HashMap;

    fn node(id: &str) -> PipelineNode {
        PipelineNode {
            id: id.to_string(),
            node_type: "noop".to_string(),
            config: serde_json::json!({}),
        }
    }

    fn pipeline(nodes: Vec<&str>, edges: Vec<(&str, &str)>) -> PipelineDefinition {
        let mut sources = HashMap::new();
        sources.insert(
            "SRC".to_string(),
            SourceConfig {
                alias: "SRC".to_string(),
                source_type: SourceType::PostgreSQL,
                partition_key: None,
                partition_hint: None,
            },
        );
        PipelineDefinition {
            pipeline_id: "p".to_string(),
            pipeline_version: 1,
            nodes: nodes.into_iter().map(node).collect(),
            edges: edges
                .into_iter()
                .map(|(s, t)| PipelineEdge {
                    source: s.to_string(),
                    target: t.to_string(),
                })
                .collect(),
            sources,
            execution: ExecutionConfig::default(),
        }
    }

    #[test]
    fn test_dag_single_node() {
        let plan = resolve_execution_levels(&pipeline(vec!["a"], vec![])).unwrap();
        assert_eq!(plan.levels, vec![vec!["a".to_string()]]);
    }

    #[test]
    fn test_dag_parallel_detection() {
        let plan = resolve_execution_levels(&pipeline(vec!["a", "b", "c"], vec![("a", "c")])).unwrap();
        assert_eq!(plan.levels.len(), 2);
        let first_level: HashSet<String> = plan.levels[0].iter().cloned().collect();
        assert!(first_level.contains("a"));
        assert!(first_level.contains("b"));
    }

    #[test]
    fn test_dag_linear_order() {
        let plan =
            resolve_execution_levels(&pipeline(vec!["a", "b", "c"], vec![("a", "b"), ("b", "c")]))
                .unwrap();
        assert_eq!(
            plan.levels,
            vec![
                vec!["a".to_string()],
                vec!["b".to_string()],
                vec!["c".to_string()]
            ]
        );
    }

    #[test]
    fn test_dag_cycle_returns_error() {
        let result =
            resolve_execution_levels(&pipeline(vec!["a", "b"], vec![("a", "b"), ("b", "a")]));
        assert!(result.is_err());
    }
}
