use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::cluster::session::SessionBuilder;
use cdrs_tokio::cluster::session::TcpSessionBuilder;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use crossql_shared::{PipelineDefinition, SourceType};
use mongodb::Client;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::env;

pub type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

#[derive(Debug)]
pub enum ConnectorError {
    MissingEnv { alias: String, key: String },
    InvalidEnv { alias: String, key: String, value: String },
    Connect { alias: String, message: String },
}

pub struct ConnectorPool {
    pub postgres: HashMap<String, PgPool>,
    pub mysql: HashMap<String, MySqlPool>,
    pub mongo: HashMap<String, Client>,
    pub cassandra: HashMap<String, CassandraSession>,
}

impl ConnectorPool {
    pub async fn build(definition: &PipelineDefinition) -> Result<Self, ConnectorError> {
        let mut postgres = HashMap::new();
        let mut mysql = HashMap::new();
        let mut mongo = HashMap::new();
        let mut cassandra = HashMap::new();

        for source in definition.sources.values() {
            match source.source_type {
                SourceType::PostgreSQL => {
                    let key = format!("CROSSQL_SRC_{}_URL", source.alias);
                    let url = required_env(&source.alias, &key)?;
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&url)
                        .await
                        .map_err(|e| ConnectorError::Connect {
                            alias: source.alias.clone(),
                            message: format!("Failed to connect PostgreSQL: {}", e),
                        })?;
                    postgres.insert(source.alias.clone(), pool);
                }
                SourceType::MySQL => {
                    let key = format!("CROSSQL_SRC_{}_URL", source.alias);
                    let url = required_env(&source.alias, &key)?;
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&url)
                        .await
                        .map_err(|e| ConnectorError::Connect {
                            alias: source.alias.clone(),
                            message: format!("Failed to connect MySQL: {}", e),
                        })?;
                    mysql.insert(source.alias.clone(), pool);
                }
                SourceType::MongoDB => {
                    let key = format!("CROSSQL_SRC_{}_URL", source.alias);
                    let url = required_env(&source.alias, &key)?;
                    let client =
                        Client::with_uri_str(&url)
                            .await
                            .map_err(|e| ConnectorError::Connect {
                                alias: source.alias.clone(),
                                message: format!("Failed to connect MongoDB: {}", e),
                            })?;
                    mongo.insert(source.alias.clone(), client);
                }
                SourceType::Cassandra => {
                    let host_key = format!("CROSSQL_SRC_{}_HOST", source.alias);
                    let port_key = format!("CROSSQL_SRC_{}_PORT", source.alias);
                    let host = required_env(&source.alias, &host_key)?;
                    let port_value = required_env(&source.alias, &port_key)?;
                    let port: u16 = port_value.parse().map_err(|_| ConnectorError::InvalidEnv {
                        alias: source.alias.clone(),
                        key: port_key.clone(),
                        value: port_value.clone(),
                    })?;
                    let session = connect_cassandra(&source.alias, &host, port).await?;
                    cassandra.insert(source.alias.clone(), session);
                }
            }
        }

        Ok(Self {
            postgres,
            mysql,
            mongo,
            cassandra,
        })
    }
}

fn required_env(alias: &str, key: &str) -> Result<String, ConnectorError> {
    env::var(key).map_err(|_| ConnectorError::MissingEnv {
        alias: alias.to_string(),
        key: key.to_string(),
    })
}

async fn connect_cassandra(
    alias: &str,
    host: &str,
    port: u16,
) -> Result<CassandraSession, ConnectorError> {
    let addr = format!("{}:{}", host, port);
    let node_config = NodeTcpConfigBuilder::new()
        .with_contact_point(addr.as_str().into())
        .build()
        .await
        .map_err(|e| ConnectorError::Connect {
            alias: alias.to_string(),
            message: format!("Failed to build Cassandra node config: {}", e),
        })?;

    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), node_config)
        .build()
        .await
        .map_err(|e| ConnectorError::Connect {
            alias: alias.to_string(),
            message: format!("Failed to connect Cassandra: {}", e),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossql_shared::{ExecutionConfig, PipelineDefinition, PipelineNode, SourceConfig};

    fn postgres_definition(alias: &str) -> PipelineDefinition {
        let mut sources = HashMap::new();
        sources.insert(
            alias.to_string(),
            SourceConfig {
                alias: alias.to_string(),
                source_type: SourceType::PostgreSQL,
                partition_key: None,
                partition_hint: None,
            },
        );
        PipelineDefinition {
            pipeline_id: "p".to_string(),
            pipeline_version: 1,
            nodes: vec![PipelineNode {
                id: "n1".to_string(),
                node_type: "postgres".to_string(),
                config: serde_json::json!({}),
            }],
            edges: vec![],
            sources,
            execution: ExecutionConfig::default(),
        }
    }

    #[tokio::test]
    async fn test_missing_env_returns_typed_error() {
        let alias = "UNITTEST_PG";
        let key = format!("CROSSQL_SRC_{}_URL", alias);
        std::env::remove_var(&key);
        let result = ConnectorPool::build(&postgres_definition(alias)).await;
        match result {
            Err(ConnectorError::MissingEnv { alias: a, key: k }) => {
                assert_eq!(a, alias);
                assert_eq!(k, key);
            }
            _ => panic!("expected missing env error"),
        }
    }
}
