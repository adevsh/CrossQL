use crossql_shared::{ExecutionConfig, SourceConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkDescriptor {
    FullFetch {
        alias: String,
    },
    RangeFetch {
        alias: String,
        partition_key: String,
        start: u64,
        end: u64,
    },
}

pub fn schedule_chunks(source: &SourceConfig, execution: &ExecutionConfig) -> Vec<ChunkDescriptor> {
    if let Some(key) = source.partition_key.clone() {
        if let Some(hint) = source.partition_hint {
            if hint > 0 {
                let mut start = 0_u64;
                let mut out = Vec::new();
                while start < hint {
                    let end = (start + execution.chunk_size).min(hint);
                    out.push(ChunkDescriptor::RangeFetch {
                        alias: source.alias.clone(),
                        partition_key: key.clone(),
                        start,
                        end,
                    });
                    start = end;
                }
                return out;
            }
        }
    }
    vec![ChunkDescriptor::FullFetch {
        alias: source.alias.clone(),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossql_shared::SourceType;

    fn source(partition_key: Option<&str>, partition_hint: Option<u64>) -> SourceConfig {
        SourceConfig {
            alias: "POSTGRES".to_string(),
            source_type: SourceType::PostgreSQL,
            partition_key: partition_key.map(str::to_string),
            partition_hint,
        }
    }

    #[test]
    fn test_chunk_split_by_partition_key() {
        let chunks = schedule_chunks(
            &source(Some("id"), Some(100_000)),
            &ExecutionConfig {
                chunk_size: 50_000,
                max_concurrency: 4,
                streaming: true,
            },
        );
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks[0],
            ChunkDescriptor::RangeFetch {
                alias: "POSTGRES".to_string(),
                partition_key: "id".to_string(),
                start: 0,
                end: 50_000
            }
        );
        assert_eq!(
            chunks[1],
            ChunkDescriptor::RangeFetch {
                alias: "POSTGRES".to_string(),
                partition_key: "id".to_string(),
                start: 50_000,
                end: 100_000
            }
        );
    }

    #[test]
    fn test_chunk_no_partition_key() {
        let chunks = schedule_chunks(&source(None, Some(100_000)), &ExecutionConfig::default());
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0],
            ChunkDescriptor::FullFetch {
                alias: "POSTGRES".to_string()
            }
        );
    }

    #[test]
    fn test_chunk_boundary_alignment() {
        let chunks = schedule_chunks(
            &source(Some("id"), Some(120_000)),
            &ExecutionConfig {
                chunk_size: 50_000,
                max_concurrency: 4,
                streaming: true,
            },
        );

        let mut last_end = 0_u64;
        for chunk in chunks {
            match chunk {
                ChunkDescriptor::RangeFetch { start, end, .. } => {
                    assert_eq!(start, last_end);
                    assert!(end > start);
                    last_end = end;
                }
                _ => panic!("expected range fetch"),
            }
        }
        assert_eq!(last_end, 120_000);
    }
}
