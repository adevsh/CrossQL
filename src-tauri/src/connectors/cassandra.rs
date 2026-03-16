use cdrs_tokio::cluster::session::TcpSessionBuilder;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::cluster::session::SessionBuilder;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::frame::message_result::ColType;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::types::ByName;
use cdrs_tokio::types::rows::Row;
use chrono::prelude::{DateTime, Utc};
use polars::prelude::{Column as PolarsColumn, *};
use cdrs_tokio::transport::TransportTcp;

pub struct CassandraConnector;

impl CassandraConnector {
    pub async fn fetch_dataframe(
        contact_points: &str,
        keyspace: &str,
        query: &str,
    ) -> Result<DataFrame, String> {
        let session = connect(contact_points).await?;

        if !keyspace.trim().is_empty() {
            session
                .query(format!("USE {}", keyspace.trim()))
                .await
                .map_err(|e| format!("Failed to set keyspace: {}", e))?;
        }

        let preview_params = StatementParamsBuilder::new().with_page_size(1).build();
        let preview_body = session
            .query_with_params(query, preview_params)
            .await
            .and_then(|envelope| envelope.response_body())
            .map_err(|e| format!("Failed to execute CQL query: {}", e))?;

        let col_specs = preview_body
            .as_rows_metadata()
            .ok_or_else(|| "Query did not return rows metadata".to_string())?
            .col_specs
            .clone();

        let mut rows: Vec<Row> = Vec::new();
        let mut pager_session = session.paged(5000);
        let mut pager = pager_session.query(query);
        loop {
            let mut page_rows = pager
                .next()
                .await
                .map_err(|e| format!("Failed to read Cassandra page: {}", e))?;
            rows.append(&mut page_rows);
            if !pager.has_more() {
                break;
            }
        }

        if rows.is_empty() {
            let empty_columns: Vec<PolarsColumn> = col_specs
                .iter()
                .map(|spec| {
                    let s = match spec.col_type.id {
                        ColType::Bigint
                        | ColType::Counter
                        | ColType::Int
                        | ColType::Smallint
                        | ColType::Tinyint
                        | ColType::Varint => {
                            Series::new_empty(spec.name.as_str().into(), &DataType::Int64)
                        }
                        ColType::Float | ColType::Double | ColType::Decimal => {
                            Series::new_empty(spec.name.as_str().into(), &DataType::Float64)
                        }
                        ColType::Boolean => {
                            Series::new_empty(spec.name.as_str().into(), &DataType::Boolean)
                        }
                        ColType::Timestamp => Series::new_empty(
                            spec.name.as_str().into(),
                            &DataType::Datetime(TimeUnit::Milliseconds, None),
                        ),
                        _ => Series::new_empty(spec.name.as_str().into(), &DataType::String),
                    };
                    PolarsColumn::from(s)
                })
                .collect();
            return DataFrame::new(0, empty_columns)
                .map_err(|e| format!("Failed to create DataFrame: {}", e));
        }

        let height = rows.len();
        let mut series_vec: Vec<Series> = Vec::with_capacity(col_specs.len());

        for spec in &col_specs {
            let col_name = spec.name.as_str();
            let col_type = spec.col_type.id;

            match col_type {
                ColType::Bigint | ColType::Counter => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| r.by_name::<i64>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Int => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| r.by_name::<i32>(col_name).ok().flatten().map(i64::from))
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Smallint => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| r.by_name::<i16>(col_name).ok().flatten().map(i64::from))
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Tinyint => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| r.by_name::<i8>(col_name).ok().flatten().map(i64::from))
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Float => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|r| r.by_name::<f32>(col_name).ok().flatten().map(f64::from))
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Double => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|r| r.by_name::<f64>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|r| r.by_name::<bool>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Timestamp => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| {
                            r.by_name::<DateTime<Utc>>(col_name)
                                .ok()
                                .flatten()
                                .map(|dt| dt.timestamp_millis())
                        })
                        .collect();
                    let mut s = Series::new(col_name.into(), values);
                    s = s
                        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                        .map_err(|e| format!("Failed to cast {} to Datetime: {}", col_name, e))?;
                    series_vec.push(s);
                }
                ColType::Uuid | ColType::Timeuuid => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|r| {
                            r.by_name::<uuid::Uuid>(col_name)
                                .ok()
                                .flatten()
                                .map(|u| u.to_string())
                        })
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                ColType::Varchar | ColType::Ascii => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|r| r.by_name::<String>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                _ => {
                    let values: Vec<Option<String>> = rows.iter().map(|_| None).collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
            }
        }

        let columns: Vec<PolarsColumn> = series_vec.into_iter().map(PolarsColumn::from).collect();
        DataFrame::new(height, columns).map_err(|e| format!("Failed to create DataFrame: {}", e))
    }

    pub async fn describe_schema(
        contact_points: &str,
        keyspace: &str,
        query: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let session = connect(contact_points).await?;

        if !keyspace.trim().is_empty() {
            session
                .query(format!("USE {}", keyspace.trim()))
                .await
                .map_err(|e| format!("Failed to set keyspace: {}", e))?;
        }

        let params = StatementParamsBuilder::new().with_page_size(1).build();

        let body = session
            .query_with_params(query, params)
            .await
            .and_then(|envelope| envelope.response_body())
            .map_err(|e| format!("Failed to execute CQL query: {}", e))?;

        let metadata = body
            .as_rows_metadata()
            .ok_or_else(|| "Query did not return rows metadata".to_string())?;

        Ok(metadata
            .col_specs
            .iter()
            .map(|c| (c.name.clone(), map_col_type_to_polars(c.col_type.id).to_string()))
            .collect())
    }
}

async fn connect(
    contact_points: &str,
) -> Result<
    Session<
        TransportTcp,
        TcpConnectionManager,
        RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
    >,
    String,
> {
    let addrs: Vec<cdrs_tokio::cluster::NodeAddress> = contact_points
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.contains(':') {
                s.to_string()
            } else {
                format!("{}:9042", s)
            }
        })
        .map(|s| cdrs_tokio::cluster::NodeAddress::from(s))
        .collect();

    if addrs.is_empty() {
        return Err("Cassandra contact_points is required".to_string());
    }

    let node_config = NodeTcpConfigBuilder::new()
        .with_contact_points(addrs)
        .build()
        .await
        .map_err(|e| format!("Failed to build Cassandra node config: {}", e))?;

    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), node_config)
        .build()
        .await
        .map_err(|e| format!("Failed to connect to Cassandra: {}", e))
}

fn map_col_type_to_polars(t: ColType) -> &'static str {
    match t {
        ColType::Uuid | ColType::Timeuuid => "Utf8",
        ColType::Timestamp => "Datetime",
        ColType::Int
        | ColType::Bigint
        | ColType::Counter
        | ColType::Smallint
        | ColType::Tinyint
        | ColType::Varint => "Int64",
        ColType::Float | ColType::Double | ColType::Decimal => "Float64",
        ColType::Boolean => "Boolean",
        ColType::Varchar | ColType::Ascii => "Utf8",
        _ => "Utf8",
    }
}
