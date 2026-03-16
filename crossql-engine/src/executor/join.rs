use polars::lazy::prelude::{col, JoinArgs, JoinType, LazyFrame};
#[cfg(test)]
use polars::prelude::{Column as PolarsColumn, DataFrame, PolarsError, Series};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

pub fn join_lazy_frames(
    left: LazyFrame,
    right: LazyFrame,
    left_on: &str,
    right_on: &str,
    kind: JoinKind,
) -> Result<LazyFrame, String> {
    let left_schema = left
        .clone()
        .collect_schema()
        .map_err(|e| format!("Failed to inspect left schema: {}", e))?;
    let right_schema = right
        .clone()
        .collect_schema()
        .map_err(|e| format!("Failed to inspect right schema: {}", e))?;

    let left_dtype = left_schema
        .get(left_on)
        .ok_or_else(|| format!("Left join key not found: {}", left_on))?;
    let right_dtype = right_schema
        .get(right_on)
        .ok_or_else(|| format!("Right join key not found: {}", right_on))?;

    if left_dtype != right_dtype {
        return Err(format!(
            "Join key type mismatch: left {} vs right {}",
            left_dtype, right_dtype
        ));
    }

    let join_type = match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
    };

    Ok(left.join(
        right,
        [col(left_on)],
        [col(right_on)],
        JoinArgs::new(join_type),
    ))
}

#[cfg(test)]
fn df_from_series(series: Vec<Series>) -> Result<DataFrame, PolarsError> {
    let columns: Vec<PolarsColumn> = series.into_iter().map(Into::into).collect();
    let height = if columns.is_empty() { 0 } else { columns[0].len() };
    DataFrame::new(height, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::lazy::prelude::IntoLazy;
    use polars::prelude::NamedFrom;

    #[test]
    fn test_inner_join_matching_rows() {
        let left = df_from_series(vec![
            Series::new("id".into(), &[1_i64, 2, 3]),
            Series::new("l".into(), &["a", "b", "c"]),
        ])
        .unwrap()
        .lazy();
        let right = df_from_series(vec![
            Series::new("id".into(), &[2_i64, 3, 4]),
            Series::new("r".into(), &["x", "y", "z"]),
        ])
        .unwrap()
        .lazy();

        let out = join_lazy_frames(left, right, "id", "id", JoinKind::Inner)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(out.height(), 2);
    }

    #[test]
    fn test_left_join_preserves_left_rows() {
        let left = df_from_series(vec![
            Series::new("id".into(), &[1_i64, 2, 3]),
            Series::new("l".into(), &["a", "b", "c"]),
        ])
        .unwrap()
        .lazy();
        let right = df_from_series(vec![
            Series::new("id".into(), &[2_i64, 3]),
            Series::new("r".into(), &["x", "y"]),
        ])
        .unwrap()
        .lazy();

        let out = join_lazy_frames(left, right, "id", "id", JoinKind::Left)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(out.height(), 3);
    }

    #[test]
    fn test_right_join_preserves_right_rows() {
        let left = df_from_series(vec![
            Series::new("id".into(), &[2_i64, 3]),
            Series::new("l".into(), &["b", "c"]),
        ])
        .unwrap()
        .lazy();
        let right = df_from_series(vec![
            Series::new("id".into(), &[1_i64, 2, 3]),
            Series::new("r".into(), &["x", "y", "z"]),
        ])
        .unwrap()
        .lazy();

        let out = join_lazy_frames(left, right, "id", "id", JoinKind::Right)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(out.height(), 3);
    }

    #[test]
    fn test_join_mismatched_key_types_returns_error() {
        let left = df_from_series(vec![
            Series::new("id".into(), &[1_i64, 2, 3]),
            Series::new("l".into(), &["a", "b", "c"]),
        ])
        .unwrap()
        .lazy();
        let right = df_from_series(vec![
            Series::new("id".into(), &["1", "2", "3"]),
            Series::new("r".into(), &["x", "y", "z"]),
        ])
        .unwrap()
        .lazy();

        let result = join_lazy_frames(left, right, "id", "id", JoinKind::Inner);
        assert!(result.is_err());
    }
}
