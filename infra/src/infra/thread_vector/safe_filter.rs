use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};

/// Type-safe filter builder for thread LanceDB queries.
/// Extends the memory SafeFilter pattern with string equality,
/// updated_at range, and per-element array_contains for labels.
#[derive(Clone)]
pub struct ThreadSafeFilter {
    /// DataFusion Expr-based filter (scalar columns)
    expr: Option<Expr>,
    /// Raw SQL fragments for LanceDB-specific functions (e.g. array_contains)
    /// that cannot be expressed via DataFusion Expr.
    raw_sql_parts: Vec<String>,
}

impl ThreadSafeFilter {
    fn from_expr(expr: Expr) -> Self {
        Self {
            expr: Some(expr),
            raw_sql_parts: vec![],
        }
    }

    pub fn thread_id(id: i64) -> Self {
        Self::from_expr(col("thread_id").eq(lit(ScalarValue::Int64(Some(id)))))
    }

    /// Match a single chunk position (N-row schema). Used to pick the
    /// representative chunk-0 row for distinct-thread counting / scalar
    /// sync.
    pub fn chunk_index(idx: i32) -> Self {
        Self::from_expr(col("chunk_index").eq(lit(ScalarValue::Int32(Some(idx)))))
    }

    pub fn user_id(id: i64) -> Self {
        Self::from_expr(col("user_id").eq(lit(ScalarValue::Int64(Some(id)))))
    }

    pub fn channel(ch: &str) -> Self {
        Self::from_expr(col("channel").eq(lit(ScalarValue::Utf8(Some(ch.to_string())))))
    }

    pub fn created_after(ts: i64) -> Self {
        Self::from_expr(col("created_at").gt(lit(ScalarValue::Int64(Some(ts)))))
    }

    pub fn created_before(ts: i64) -> Self {
        Self::from_expr(col("created_at").lt(lit(ScalarValue::Int64(Some(ts)))))
    }

    pub fn updated_after(ts: i64) -> Self {
        Self::from_expr(col("updated_at").gt(lit(ScalarValue::Int64(Some(ts)))))
    }

    pub fn updated_before(ts: i64) -> Self {
        Self::from_expr(col("updated_at").lt(lit(ScalarValue::Int64(Some(ts)))))
    }

    /// Filter threads that have ANY of the specified labels.
    /// Expanded to OR-chain of array_contains because lance-datafusion does not
    /// expose Spark's array_contains_any (only array_contains via array_has alias).
    pub fn labels_any(labels: &[String]) -> Self {
        Self::labels_match(labels, " OR ")
    }

    /// Filter threads that have ALL of the specified labels.
    /// Expanded to AND-chain of array_contains; see labels_any for rationale.
    pub fn labels_all(labels: &[String]) -> Self {
        Self::labels_match(labels, " AND ")
    }

    fn labels_match(labels: &[String], joiner: &str) -> Self {
        if labels.is_empty() {
            return Self {
                expr: None,
                raw_sql_parts: vec![],
            };
        }
        let clauses: Vec<String> = labels
            .iter()
            .map(|l| format!("array_contains(labels, '{}')", l.replace('\'', "''")))
            .collect();
        // Always wrap in parentheses so this fragment composes safely with the
        // outer AND join performed in `to_sql`.
        let sql = format!("({})", clauses.join(joiner));
        Self {
            expr: None,
            raw_sql_parts: vec![sql],
        }
    }

    /// Build IN list filter for i64 columns.
    pub fn in_i64_list(column: &str, values: &[i64]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("in_i64_list: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Int64(Some(v))))
            .collect();
        Ok(Self::from_expr(col(column).in_list(literals, false)))
    }

    /// Build IN list filter for Utf8 columns (e.g. `vector_kind` for the
    /// N-row replace_kinds delete).
    pub fn in_str_list(column: &str, values: &[&str]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("in_str_list: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Utf8(Some(v.to_string()))))
            .collect();
        Ok(Self::from_expr(col(column).in_list(literals, false)))
    }

    pub fn and(mut self, other: ThreadSafeFilter) -> Self {
        self.expr = match (self.expr, other.expr) {
            (Some(a), Some(b)) => Some(a.and(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        self.raw_sql_parts.extend(other.raw_sql_parts);
        self
    }

    /// Build from protobuf ThreadSearchFilter.
    pub fn from_proto_filter(
        filter: &protobuf::llm_memory::data::ThreadSearchFilter,
    ) -> Option<Self> {
        let mut result: Option<Self> = None;
        let mut combine_and = |f: Self| {
            result = Some(match result.take() {
                Some(existing) => existing.and(f),
                None => f,
            });
        };

        if let Some(uid) = filter.user_id {
            combine_and(Self::user_id(uid));
        }

        if !filter.labels.is_empty() {
            let match_all = filter.label_match_mode
                == Some(protobuf::llm_memory::data::LabelMatchMode::LabelAll as i32);
            if match_all {
                combine_and(Self::labels_all(&filter.labels));
            } else {
                combine_and(Self::labels_any(&filter.labels));
            }
        }

        if let Some(ch) = &filter.channel {
            combine_and(Self::channel(ch));
        }

        if let Some(after) = filter.created_after {
            combine_and(Self::created_after(after));
        }
        if let Some(before) = filter.created_before {
            combine_and(Self::created_before(before));
        }
        if let Some(after) = filter.updated_after {
            combine_and(Self::updated_after(after));
        }
        if let Some(before) = filter.updated_before {
            combine_and(Self::updated_before(before));
        }

        result
    }

    /// Convert to LanceDB SQL filter string.
    pub fn to_sql(&self) -> anyhow::Result<String> {
        let mut parts: Vec<String> = Vec::new();

        if let Some(ref expr) = self.expr {
            parts.push(expr_to_safe_string(expr)?);
        }
        parts.extend(self.raw_sql_parts.clone());

        if parts.is_empty() {
            anyhow::bail!("ThreadSafeFilter is empty");
        }
        Ok(parts.join(" AND "))
    }
}

/// Convert DataFusion Expr to safe SQL string.
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
                _ => anyhow::bail!("Unsupported operator: {:?}", binary.op),
            };
            Ok(format!("({left} {op} {right})"))
        }
        Expr::Column(c) => Ok(c.name.clone()),
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
            ScalarValue::Utf8(Some(s)) => Ok(format!("'{}'", s.replace('\'', "''"))),
            _ => anyhow::bail!("Unsupported scalar: {:?}", sv),
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
        _ => anyhow::bail!("Unsupported expr: {:?}", expr),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_scalar_filters() {
        let f = ThreadSafeFilter::user_id(42);
        assert_eq!(f.to_sql().unwrap(), "(user_id = 42)");

        let f = ThreadSafeFilter::channel("discord");
        assert_eq!(f.to_sql().unwrap(), "(channel = 'discord')");

        let f = ThreadSafeFilter::updated_after(1000);
        assert_eq!(f.to_sql().unwrap(), "(updated_at > 1000)");
    }

    #[test]
    fn test_labels_any() {
        let f = ThreadSafeFilter::labels_any(&["rust".to_string(), "tokio".to_string()]);
        let sql = f.to_sql().unwrap();
        assert_eq!(
            sql,
            "(array_contains(labels, 'rust') OR array_contains(labels, 'tokio'))"
        );
    }

    #[test]
    fn test_labels_all() {
        let f = ThreadSafeFilter::labels_all(&["rust".to_string(), "async".to_string()]);
        let sql = f.to_sql().unwrap();
        assert_eq!(
            sql,
            "(array_contains(labels, 'rust') AND array_contains(labels, 'async'))"
        );
    }

    #[test]
    fn test_label_escaping() {
        let f = ThreadSafeFilter::labels_any(&["it's a label".to_string()]);
        let sql = f.to_sql().unwrap();
        assert_eq!(sql, "(array_contains(labels, 'it''s a label'))");
    }

    #[test]
    fn test_combined_filter() {
        let f = ThreadSafeFilter::user_id(1)
            .and(ThreadSafeFilter::labels_any(&["rust".to_string()]))
            .and(ThreadSafeFilter::created_after(1000));
        let sql = f.to_sql().unwrap();
        assert!(sql.contains("user_id = 1"));
        assert!(sql.contains("(array_contains(labels, 'rust'))"));
        assert!(sql.contains("created_at > 1000"));
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_empty_labels_ignored() {
        let f = ThreadSafeFilter::labels_any(&[]);
        assert!(f.to_sql().is_err()); // empty filter
    }

    #[test]
    fn test_from_proto_filter() {
        let pf = protobuf::llm_memory::data::ThreadSearchFilter {
            user_id: Some(10),
            labels: vec!["test".to_string()],
            label_match_mode: Some(0), // ANY
            channel: Some("slack".to_string()),
            created_after: Some(500),
            created_before: None,
            updated_after: None,
            updated_before: Some(2000),
        };
        let sf = ThreadSafeFilter::from_proto_filter(&pf).unwrap();
        let sql = sf.to_sql().unwrap();
        assert!(sql.contains("user_id = 10"));
        assert!(sql.contains("(array_contains(labels, 'test'))"));
        assert!(sql.contains("channel = 'slack'"));
        assert!(sql.contains("created_at > 500"));
        assert!(sql.contains("updated_at < 2000"));
    }

    #[test]
    fn test_in_i64_list() {
        let f = ThreadSafeFilter::in_i64_list("thread_id", &[1, 2, 3]).unwrap();
        assert_eq!(f.to_sql().unwrap(), "(thread_id IN (1, 2, 3))");
    }
}
