use crossql_lib::engine::transform::{DerivedColumnConfig, FilterConfig, Transformer};
use polars::df;
use polars::prelude::*;

#[doc = "Expected: numeric equality filter keeps only matching rows.\nBehavior: apply_filter with op=eq and value_type=number returns rows where column equals value."]
#[test]
fn apply_filter_eq_number_filters_expected_rows() {
    let df = df!("id" => &[1_i64, 2, 2, 3], "name" => &["a", "b", "c", "d"]).unwrap();
    let lf = df.lazy();
    let cfg = FilterConfig {
        column: Some("id".to_string()),
        op: Some("eq".to_string()),
        value_type: Some("number".to_string()),
        value: Some("2".to_string()),
    };

    let out = Transformer::apply_filter(lf, cfg).unwrap().collect().unwrap();
    let ids: Vec<i64> = out
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();

    assert_eq!(out.height(), 2);
    assert_eq!(ids, vec![2, 2]);
}

#[doc = "Expected: unsupported filter operators are rejected.\nBehavior: apply_filter returns an error message for unknown op strings."]
#[test]
fn apply_filter_rejects_unknown_operator() {
    let df = df!("id" => &[1_i64, 2, 3]).unwrap();
    let lf = df.lazy();
    let cfg = FilterConfig {
        column: Some("id".to_string()),
        op: Some("unknown_op".to_string()),
        value_type: Some("number".to_string()),
        value: Some("2".to_string()),
    };

    let out = Transformer::apply_filter(lf, cfg);
    match out {
        Err(e) => assert_eq!(e, "Unsupported filter operator: unknown_op"),
        Ok(_) => panic!("expected unsupported operator error"),
    }
}

#[doc = "Expected: derived add with literal creates a new computed column.\nBehavior: apply_derived with op=add and right_kind=literal adds the literal to each left value."]
#[test]
fn apply_derived_add_literal_creates_expected_column() {
    let df = df!("base" => &[10_i64, 20, 30]).unwrap();
    let lf = df.lazy();
    let cfg = DerivedColumnConfig {
        name: Some("sum".to_string()),
        op: Some("add".to_string()),
        left: Some("base".to_string()),
        right_kind: Some("literal".to_string()),
        right: Some("5".to_string()),
    };

    let out = Transformer::apply_derived(lf, cfg).unwrap().collect().unwrap();
    let values: Vec<i64> = out
        .column("sum")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();

    assert_eq!(values, vec![15, 25, 35]);
}
