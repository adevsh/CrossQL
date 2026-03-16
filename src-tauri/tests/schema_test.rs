use crossql_lib::engine::schema::{
    apply_schema_maps, validate_no_nulls, SchemaMapColumnConfig, SchemaMapConfig,
};
use polars::df;
use polars::prelude::*;

#[doc = "Expected: fill_default replaces nulls using configured typed default.\nBehavior: apply_schema_maps with null_mode=fill_default and cast=Int64 fills null entries with provided value."]
#[test]
fn apply_schema_maps_fill_default_replaces_nulls() {
    let df = df!("age" => &[Some(1_i64), None, Some(3)]).unwrap();
    let lf = df.lazy();
    let cfg = SchemaMapConfig {
        columns: vec![SchemaMapColumnConfig {
            source: "age".to_string(),
            rename: None,
            cast: Some("Int64".to_string()),
            null_mode: Some("fill_default".to_string()),
            fill_value: Some("0".to_string()),
        }],
    };

    let (out_lf, error_cols) = apply_schema_maps(lf, &[cfg]).unwrap();
    let out = out_lf.collect().unwrap();
    let values: Vec<i64> = out
        .column("age")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();

    assert!(error_cols.is_empty());
    assert_eq!(values, vec![1, 0, 3]);
}

#[doc = "Expected: error mode tracks the target column for post-run null validation.\nBehavior: apply_schema_maps returns the renamed column in error_on_null_cols when null_mode=error."]
#[test]
fn apply_schema_maps_error_mode_tracks_renamed_target_column() {
    let df = df!("age" => &[Some(1_i64), None, Some(3)]).unwrap();
    let lf = df.lazy();
    let cfg = SchemaMapConfig {
        columns: vec![SchemaMapColumnConfig {
            source: "age".to_string(),
            rename: Some("age_norm".to_string()),
            cast: None,
            null_mode: Some("error".to_string()),
            fill_value: None,
        }],
    };

    let (out_lf, error_cols) = apply_schema_maps(lf, &[cfg]).unwrap();
    let out = out_lf.collect().unwrap();

    assert!(out.column("age_norm").is_ok());
    assert_eq!(error_cols, vec!["age_norm".to_string()]);
}

#[doc = "Expected: null validation fails when required columns contain nulls.\nBehavior: validate_no_nulls returns a descriptive error including null count for offending column."]
#[test]
fn validate_no_nulls_detects_null_columns() {
    let df = df!("required_col" => &[Some(1_i64), None, Some(2)]).unwrap();
    let required = vec!["required_col".to_string()];

    let result = validate_no_nulls(&df, &required);

    assert_eq!(
        result,
        Err("Column 'required_col' contains 1 null values".to_string())
    );
}
