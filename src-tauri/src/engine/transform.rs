use polars::lazy::prelude::*;
use polars::prelude::*;
use super::types::parse_dtype;

#[derive(serde::Deserialize)]
pub struct FilterConfig {
    pub column: Option<String>,
    pub op: Option<String>,
    #[serde(rename = "value_type")]
    pub value_type: Option<String>,
    pub value: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct SelectConfig {
    pub columns: Vec<String>,
}

#[derive(serde::Deserialize)]
pub struct RenameMapping {
    pub from: String,
    pub to: String,
}

#[derive(serde::Deserialize)]
pub struct RenameConfig {
    pub mappings: Vec<RenameMapping>,
}

#[derive(serde::Deserialize)]
pub struct CastSpec {
    pub column: String,
    pub dtype: String,
}

#[derive(serde::Deserialize)]
pub struct CastConfig {
    pub casts: Vec<CastSpec>,
}

#[derive(serde::Deserialize)]
pub struct DerivedColumnConfig {
    pub name: Option<String>,
    pub op: Option<String>,
    pub left: Option<String>,
    pub right_kind: Option<String>,
    pub right: Option<String>,
}

fn parse_value(value_type: &str, value: &str) -> Result<Expr, String> {
    match value_type {
        "number" => {
            if value.contains('.') {
                let v: f64 = value.parse().map_err(|_| "Invalid number value".to_string())?;
                Ok(lit(v))
            } else {
                let v: i64 = value.parse().map_err(|_| "Invalid number value".to_string())?;
                Ok(lit(v))
            }
        }
        "boolean" => {
            let v: bool = value.parse().map_err(|_| "Invalid boolean value".to_string())?;
            Ok(lit(v))
        }
        _ => Ok(lit(value.to_string())),
    }
}

pub struct Transformer;

impl Transformer {
    pub fn apply_filter(lf: LazyFrame, cfg: FilterConfig) -> Result<LazyFrame, String> {
        let column = cfg
            .column
            .unwrap_or_default()
            .trim()
            .to_string();
        if column.is_empty() {
            return Err("Filter requires column".to_string());
        }
        let op = cfg.op.unwrap_or_else(|| "eq".to_string());
        let vt = cfg.value_type.unwrap_or_else(|| "string".to_string());
        let value = cfg.value.unwrap_or_default();

        let expr = match op.as_str() {
            "eq" => col(&column).eq(parse_value(&vt, &value)?),
            "ne" => col(&column).neq(parse_value(&vt, &value)?),
            "gt" => col(&column).gt(parse_value(&vt, &value)?),
            "gte" => col(&column).gt_eq(parse_value(&vt, &value)?),
            "lt" => col(&column).lt(parse_value(&vt, &value)?),
            "lte" => col(&column).lt_eq(parse_value(&vt, &value)?),
            "contains" => col(&column)
                .cast(DataType::String)
                .str()
                .contains_literal(lit(value)),
            "is_null" => col(&column).is_null(),
            "is_not_null" => col(&column).is_not_null(),
            _ => return Err(format!("Unsupported filter operator: {}", op)),
        };

        Ok(lf.filter(expr))
    }

    pub fn apply_select(lf: LazyFrame, cfg: SelectConfig) -> Result<LazyFrame, String> {
        let cols: Vec<String> = cfg
            .columns
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if cols.is_empty() {
            return Err("Select requires one or more columns".to_string());
        }
        Ok(lf.select(cols.into_iter().map(|c| col(c)).collect::<Vec<_>>()))
    }

    pub fn apply_rename(lf: LazyFrame, cfg: RenameConfig) -> Result<LazyFrame, String> {
        let mut existing: Vec<String> = Vec::new();
        let mut new: Vec<String> = Vec::new();

        for m in cfg.mappings {
            let from = m.from.trim();
            let to = m.to.trim();
            if from.is_empty() || to.is_empty() || from == to {
                continue;
            }
            existing.push(from.to_string());
            new.push(to.to_string());
        }
        if existing.is_empty() {
            return Err("Rename requires one or more mappings".to_string());
        }
        Ok(lf.rename(existing, new, false))
    }

    pub fn apply_cast(lf: LazyFrame, cfg: CastConfig) -> Result<LazyFrame, String> {
        let mut exprs: Vec<Expr> = Vec::new();
        for c in cfg.casts {
            let name = c.column.trim();
            if name.is_empty() {
                continue;
            }
            let dtype = parse_dtype(c.dtype.trim())?;
            exprs.push(col(name).cast(dtype).alias(name));
        }
        if exprs.is_empty() {
            return Err("Cast requires one or more casts".to_string());
        }
        Ok(lf.with_columns(exprs))
    }

    pub fn apply_derived(lf: LazyFrame, cfg: DerivedColumnConfig) -> Result<LazyFrame, String> {
        let name = cfg
            .name
            .unwrap_or_default()
            .trim()
            .to_string();
        if name.is_empty() {
            return Err("Derived column requires name".to_string());
        }

        let op = cfg.op.unwrap_or_else(|| "upper".to_string());
        let left = cfg.left.unwrap_or_default().trim().to_string();
        if left.is_empty() {
            return Err("Derived column requires left".to_string());
        }

        let right_kind = cfg.right_kind.unwrap_or_else(|| "column".to_string());
        let right = cfg.right.unwrap_or_default();

        let expr = match op.as_str() {
            "upper" => col(&left).cast(DataType::String).str().to_uppercase(),
            "lower" => col(&left).cast(DataType::String).str().to_lowercase(),
            "add" => {
                let rhs = if right_kind == "literal" {
                    let v = right.trim();
                    if v.is_empty() {
                        return Err("Derived add requires right".to_string());
                    }
                    if v.contains('.') {
                        let f: f64 = v.parse().map_err(|_| "Invalid number literal".to_string())?;
                        lit(f)
                    } else {
                        let i: i64 = v.parse().map_err(|_| "Invalid number literal".to_string())?;
                        lit(i)
                    }
                } else {
                    let c = right.trim();
                    if c.is_empty() {
                        return Err("Derived add requires right column".to_string());
                    }
                    col(c)
                };
                col(&left) + rhs
            }
            "concat" => {
                let rhs = if right_kind == "literal" {
                    lit(right)
                } else {
                    let c = right.trim();
                    if c.is_empty() {
                        return Err("Derived concat requires right column".to_string());
                    }
                    col(c)
                };
                col(&left).cast(DataType::String) + rhs.cast(DataType::String)
            }
            _ => return Err(format!("Unsupported derived op: {}", op)),
        };

        Ok(lf.with_column(expr.alias(&name)))
    }
}
