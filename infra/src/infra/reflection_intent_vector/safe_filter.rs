//! Tiny filter helper for the intent vector store.
//!
//! Borrows the same DataFusion-Expr-based design as
//! `memory_vector::safe_filter` so we get type-safe `IN` lists and
//! AND chains. The intent table has a different column set than
//! `memory_vector` (no `content` / `user_id` / `role`), so we declare
//! only the predicates the reflection layer actually needs.

use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};

#[derive(Clone)]
pub struct IntentSafeFilter {
    expr: Expr,
}

impl IntentSafeFilter {
    pub fn memory_id(id: i64) -> Self {
        Self {
            expr: col("memory_id").eq(lit(ScalarValue::Int64(Some(id)))),
        }
    }
    pub fn origin_user_id(id: i64) -> Self {
        Self {
            expr: col("origin_user_id").eq(lit(ScalarValue::Int64(Some(id)))),
        }
    }
    pub fn task_category(c: i32) -> Self {
        Self {
            expr: col("task_category").eq(lit(ScalarValue::Int32(Some(c)))),
        }
    }
    pub fn reflection_aspect(a: i32) -> Self {
        Self {
            expr: col("reflection_aspect").eq(lit(ScalarValue::Int32(Some(a)))),
        }
    }
    pub fn outcome(o: i32) -> Self {
        Self {
            expr: col("outcome").eq(lit(ScalarValue::Int32(Some(o)))),
        }
    }

    /// Match a single chunk position (N-row schema).
    pub fn chunk_index(idx: i32) -> Self {
        Self {
            expr: col("chunk_index").eq(lit(ScalarValue::Int32(Some(idx)))),
        }
    }

    /// IN-list filter for Utf8 columns (e.g. `vector_kind` for the N-row
    /// replace_kinds delete). Mirrors `memory_vector`'s
    /// `SafeFilter::in_str_list`.
    pub fn in_str_list(column: &str, values: &[&str]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("in_str_list: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Utf8(Some(v.to_string()))))
            .collect();
        Ok(Self {
            expr: col(column).in_list(literals, false),
        })
    }

    /// IN-list filter against `task_category`. Returns Ok with an
    /// always-false placeholder when the input is empty so the
    /// caller can skip-conditional without special casing.
    pub fn task_categories(values: &[i32]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("task_categories: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Int32(Some(v))))
            .collect();
        Ok(Self {
            expr: col("task_category").in_list(literals, false),
        })
    }

    /// IN-list filter against `outcome`.
    pub fn outcomes(values: &[i32]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("outcomes: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Int32(Some(v))))
            .collect();
        Ok(Self {
            expr: col("outcome").in_list(literals, false),
        })
    }

    /// Negated single-value equality on `memory_id`. Used by F-S3 to
    /// strip the reference reflection from its own similarity result
    /// set (the trivial self-hit at distance=0 is never useful).
    pub fn memory_id_not(id: i64) -> Self {
        // `NotEq` exists in DataFusion but `expr_to_safe_string`
        // below does not whitelist it; sticking to `NOT IN (...)`
        // keeps the existing serializer working.
        Self {
            expr: col("memory_id").in_list(
                vec![lit(ScalarValue::Int64(Some(id)))],
                /* negated */ true,
            ),
        }
    }

    /// Build IN list filter against `memory_id` (primary 2-stage filter
    /// path: RDB sidecar narrows the candidate set, this clause hands
    /// the IDs to LanceDB).
    pub fn memory_id_in(values: &[i64]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("memory_id_in: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Int64(Some(v))))
            .collect();
        Ok(Self {
            expr: col("memory_id").in_list(literals, false),
        })
    }

    pub fn and(self, other: IntentSafeFilter) -> Self {
        Self {
            expr: self.expr.and(other.expr),
        }
    }

    /// Convert to a LanceDB SQL filter string. Mirrors the equivalent
    /// helper in `memory_vector::safe_filter::SafeFilter::to_sql`.
    pub fn to_sql(&self) -> anyhow::Result<String> {
        expr_to_safe_string(&self.expr)
    }
}

fn expr_to_safe_string(expr: &Expr) -> anyhow::Result<String> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = expr_to_safe_string(&binary.left)?;
            let right = expr_to_safe_string(&binary.right)?;
            let op = match binary.op {
                datafusion_expr::Operator::Eq => "=",
                datafusion_expr::Operator::And => "AND",
                datafusion_expr::Operator::Or => "OR",
                datafusion_expr::Operator::Gt => ">",
                datafusion_expr::Operator::Lt => "<",
                datafusion_expr::Operator::GtEq => ">=",
                datafusion_expr::Operator::LtEq => "<=",
                _ => anyhow::bail!("unsupported operator: {:?}", binary.op),
            };
            Ok(format!("({left} {op} {right})"))
        }
        Expr::Column(c) => Ok(c.name.clone()),
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
            ScalarValue::Utf8(Some(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
            _ => anyhow::bail!("unsupported scalar: {:?}", sv),
        },
        Expr::InList(in_list) => {
            let col_str = expr_to_safe_string(&in_list.expr)?;
            let values: Vec<String> = in_list
                .list
                .iter()
                .map(expr_to_safe_string)
                .collect::<anyhow::Result<Vec<_>>>()?;
            let neg = if in_list.negated { "NOT " } else { "" };
            Ok(format!("({col_str} {neg}IN ({}))", values.join(", ")))
        }
        _ => anyhow::bail!("unsupported expr: {:?}", expr),
    }
}
