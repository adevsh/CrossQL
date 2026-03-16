use futures_util::stream::TryStreamExt;
use mongodb::bson::{self, Bson, Document};
use mongodb::options::{FindOptions, ClientOptions};
use mongodb::Client;
use polars::prelude::{Column as PolarsColumn, *};
use std::collections::{BTreeSet, HashMap};

pub struct MongoConnector;

#[derive(Clone, Copy, PartialEq, Eq)]
enum CanonType {
    Int64,
    Float64,
    Boolean,
    DatetimeMs,
    Utf8,
}

impl MongoConnector {
    pub async fn fetch_dataframe(
        uri: &str,
        database: &str,
        collection: &str,
        filter_json: &str,
        projection_json: &str,
        flatten_depth: usize,
    ) -> Result<DataFrame, String> {
        let docs = fetch_flattened_docs(
            uri,
            database,
            collection,
            filter_json,
            projection_json,
            flatten_depth,
            None,
        )
        .await?;

        if docs.is_empty() {
            return DataFrame::new(0, Vec::new())
                .map_err(|e| format!("Failed to create DataFrame: {}", e));
        }

        let height = docs.len();
        let schema = infer_schema(&docs);
        let mut series_vec: Vec<Series> = Vec::with_capacity(schema.len());

        for (key, dtype) in schema {
            match dtype {
                CanonType::Int64 => {
                    let values: Vec<Option<i64>> =
                        docs.iter().map(|d| bson_to_i64(d.get(&key))).collect();
                    series_vec.push(Series::new(key.into(), values));
                }
                CanonType::Float64 => {
                    let values: Vec<Option<f64>> =
                        docs.iter().map(|d| bson_to_f64(d.get(&key))).collect();
                    series_vec.push(Series::new(key.into(), values));
                }
                CanonType::Boolean => {
                    let values: Vec<Option<bool>> =
                        docs.iter().map(|d| bson_to_bool(d.get(&key))).collect();
                    series_vec.push(Series::new(key.into(), values));
                }
                CanonType::DatetimeMs => {
                    let values: Vec<Option<i64>> =
                        docs.iter().map(|d| bson_to_datetime_ms(d.get(&key))).collect();
                    let mut s = Series::new(key.as_str().into(), values);
                    s = s
                        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                        .map_err(|e| format!("Failed to cast {} to Datetime: {}", key, e))?;
                    series_vec.push(s);
                }
                CanonType::Utf8 => {
                    let values: Vec<Option<String>> =
                        docs.iter().map(|d| bson_to_string(d.get(&key))).collect();
                    series_vec.push(Series::new(key.into(), values));
                }
            }
        }

        let columns: Vec<PolarsColumn> = series_vec.into_iter().map(PolarsColumn::from).collect();
        DataFrame::new(height, columns).map_err(|e| format!("Failed to create DataFrame: {}", e))
    }

    pub async fn describe_schema(
        uri: &str,
        database: &str,
        collection: &str,
        filter_json: &str,
        projection_json: &str,
        flatten_depth: usize,
    ) -> Result<Vec<(String, String)>, String> {
        let docs = fetch_flattened_docs(
            uri,
            database,
            collection,
            filter_json,
            projection_json,
            flatten_depth,
            Some(100),
        )
        .await?;

        let schema = infer_schema(&docs);
        Ok(schema
            .into_iter()
            .map(|(k, t)| (k, canon_type_name(t).to_string()))
            .collect())
    }
}

fn canon_type_name(t: CanonType) -> &'static str {
    match t {
        CanonType::Int64 => "Int64",
        CanonType::Float64 => "Float64",
        CanonType::Boolean => "Boolean",
        CanonType::DatetimeMs => "Datetime",
        CanonType::Utf8 => "Utf8",
    }
}

async fn fetch_flattened_docs(
    uri: &str,
    database: &str,
    collection: &str,
    filter_json: &str,
    projection_json: &str,
    flatten_depth: usize,
    limit: Option<i64>,
) -> Result<Vec<HashMap<String, Bson>>, String> {
    let opts = ClientOptions::parse(uri)
        .await
        .map_err(|e| format!("Failed to parse MongoDB URI: {}", e))?;
    let client = Client::with_options(opts).map_err(|e| format!("Failed to create Mongo client: {}", e))?;

    let filter_doc = parse_json_document(filter_json)?;
    let projection_doc = parse_json_document(projection_json)?;

    let mut find_options = FindOptions::default();
    if !projection_doc.is_empty() {
        find_options.projection = Some(projection_doc);
    }
    if let Some(l) = limit {
        find_options.limit = Some(l);
    }

    let coll = client
        .database(database)
        .collection::<Document>(collection);

    let mut cursor = coll
        .find(filter_doc)
        .with_options(find_options)
        .await
        .map_err(|e| format!("Failed to query MongoDB: {}", e))?;

    let mut out: Vec<HashMap<String, Bson>> = Vec::new();
    while let Some(doc) = cursor
        .try_next()
        .await
        .map_err(|e| format!("Failed to read MongoDB cursor: {}", e))?
    {
        out.push(flatten_document(&doc, flatten_depth));
    }

    Ok(out)
}

fn parse_json_document(input: &str) -> Result<Document, String> {
    let s = input.trim();
    if s.is_empty() {
        return Ok(Document::new());
    }
    let v: serde_json::Value =
        serde_json::from_str(s).map_err(|e| format!("Invalid JSON: {}", e))?;
    bson::to_document(&v).map_err(|e| format!("Failed to convert JSON to BSON: {}", e))
}

fn flatten_document(doc: &Document, depth: usize) -> HashMap<String, Bson> {
    let mut out = HashMap::new();
    for (k, v) in doc.iter() {
        flatten_value(&mut out, k.as_str(), v, depth);
    }
    out
}

fn flatten_value(out: &mut HashMap<String, Bson>, key: &str, value: &Bson, depth: usize) {
    if depth > 0 {
        if let Bson::Document(d) = value {
            for (k, v) in d.iter() {
                let next = format!("{}.{}", key, k);
                flatten_value(out, next.as_str(), v, depth - 1);
            }
            return;
        }
    }
    out.insert(key.to_string(), value.clone());
}

fn infer_schema(docs: &[HashMap<String, Bson>]) -> Vec<(String, CanonType)> {
    if docs.is_empty() {
        return vec![];
    }

    let mut keys = BTreeSet::new();
    for d in docs {
        for k in d.keys() {
            keys.insert(k.clone());
        }
    }

    let mut inferred: Vec<(String, CanonType)> = Vec::with_capacity(keys.len());
    for k in keys {
        let mut current: Option<CanonType> = None;
        for d in docs {
            if let Some(v) = d.get(&k) {
                if let Some(ct) = canon_of(v) {
                    current = Some(match current {
                        None => ct,
                        Some(prev) => merge_type(prev, ct),
                    });
                    if current == Some(CanonType::Utf8) {
                        break;
                    }
                }
            }
        }
        inferred.push((k, current.unwrap_or(CanonType::Utf8)));
    }

    inferred
}

fn canon_of(v: &Bson) -> Option<CanonType> {
    match v {
        Bson::Null => None,
        Bson::Int32(_) | Bson::Int64(_) => Some(CanonType::Int64),
        Bson::Double(_) => Some(CanonType::Float64),
        Bson::Boolean(_) => Some(CanonType::Boolean),
        Bson::DateTime(_) => Some(CanonType::DatetimeMs),
        Bson::String(_) | Bson::ObjectId(_) => Some(CanonType::Utf8),
        _ => Some(CanonType::Utf8),
    }
}

fn merge_type(a: CanonType, b: CanonType) -> CanonType {
    if a == b {
        return a;
    }
    match (a, b) {
        (CanonType::Int64, CanonType::Float64) | (CanonType::Float64, CanonType::Int64) => {
            CanonType::Float64
        }
        _ => CanonType::Utf8,
    }
}

fn bson_to_i64(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Null => None,
        Bson::Int32(x) => Some(i64::from(*x)),
        Bson::Int64(x) => Some(*x),
        _ => None,
    }
}

fn bson_to_f64(v: Option<&Bson>) -> Option<f64> {
    match v? {
        Bson::Null => None,
        Bson::Double(x) => Some(*x),
        Bson::Int32(x) => Some(f64::from(*x)),
        Bson::Int64(x) => Some(*x as f64),
        _ => None,
    }
}

fn bson_to_bool(v: Option<&Bson>) -> Option<bool> {
    match v? {
        Bson::Null => None,
        Bson::Boolean(x) => Some(*x),
        _ => None,
    }
}

fn bson_to_datetime_ms(v: Option<&Bson>) -> Option<i64> {
    match v? {
        Bson::Null => None,
        Bson::DateTime(dt) => Some(dt.timestamp_millis()),
        _ => None,
    }
}

fn bson_to_string(v: Option<&Bson>) -> Option<String> {
    let vv = v?;
    match vv {
        Bson::Null => None,
        Bson::String(s) => Some(s.clone()),
        Bson::ObjectId(oid) => Some(oid.to_hex()),
        _ => serde_json::to_string(vv).ok(),
    }
}
