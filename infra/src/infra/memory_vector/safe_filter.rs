use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};

/// Type-safe filter builder for LanceDB queries.
/// Uses DataFusion Expr API to prevent SQL injection.
#[derive(Clone)]
pub struct SafeFilter {
    expr: Expr,
}

impl SafeFilter {
    pub fn memory_id(id: i64) -> Self {
        Self {
            expr: col("memory_id").eq(lit(ScalarValue::Int64(Some(id)))),
        }
    }
    pub fn user_id(id: i64) -> Self {
        Self {
            expr: col("user_id").eq(lit(ScalarValue::Int64(Some(id)))),
        }
    }
    // Phase 4: `SafeFilter::thread_id` was removed together with the
    // `MemorySearchFilter.thread_id` proto field. Thread-scoped searches go
    // through the `thread_memory` junction table to resolve a memory_id set
    // first, and then narrow the vector query via
    // `SafeFilter::in_i64_list("memory_id", …)`.
    pub fn role(role: i32) -> Self {
        Self {
            expr: col("role").eq(lit(ScalarValue::Int32(Some(role)))),
        }
    }
    pub fn content_type(ct: i32) -> Self {
        Self {
            expr: col("content_type").eq(lit(ScalarValue::Int32(Some(ct)))),
        }
    }
    pub fn memory_kinds_any(kinds: &[i32]) -> anyhow::Result<Self> {
        if kinds.is_empty() {
            anyhow::bail!("memory_kinds_any: kinds must not be empty");
        }
        let literals = kinds
            .iter()
            .map(|&kind| lit(ScalarValue::Int32(Some(kind))))
            .collect();
        Ok(Self {
            expr: col("memory_kind").in_list(literals, false),
        })
    }
    pub fn created_after(ts: i64) -> Self {
        Self {
            expr: col("created_at").gt(lit(ScalarValue::Int64(Some(ts)))),
        }
    }
    pub fn created_before(ts: i64) -> Self {
        Self {
            expr: col("created_at").lt(lit(ScalarValue::Int64(Some(ts)))),
        }
    }
    pub fn updated_after(ts: i64) -> Self {
        Self {
            expr: col("updated_at").gt(lit(ScalarValue::Int64(Some(ts)))),
        }
    }
    pub fn updated_before(ts: i64) -> Self {
        Self {
            expr: col("updated_at").lt(lit(ScalarValue::Int64(Some(ts)))),
        }
    }

    /// Build IN list filter: `column IN (val1, val2, ...)`
    /// Returns error if values is empty (empty IN list is invalid SQL).
    ///
    /// The column name is unrestricted because DataFusion's `col()` constructs
    /// a typed column reference (not raw SQL), so injection is not possible.
    /// Currently only used internally with "memory_id" for hybrid search candidate filtering.
    pub fn in_i64_list(column: &str, values: &[i64]) -> anyhow::Result<Self> {
        if values.is_empty() {
            anyhow::bail!("in_i64_list: values must not be empty");
        }
        let literals: Vec<Expr> = values
            .iter()
            .map(|&v| lit(ScalarValue::Int64(Some(v))))
            .collect();
        Ok(Self {
            expr: col(column).in_list(literals, false),
        })
    }

    /// Build IN list filter over a Utf8 column: `column IN ('a','b',...)`.
    /// Returns error if `values` is empty (empty IN is invalid SQL).
    ///
    /// Injection-safe for the same reason as [`Self::in_i64_list`]:
    /// DataFusion builds a typed column reference + string literals, not
    /// raw SQL. Image memory Phase 4 uses this for the `replace_kinds`
    /// delete (`vector_kind IN (...)`) so a Text vs Media dispatch only
    /// clears its own kinds (design 1/3 §3.3.1 kind-isolation).
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

    pub fn and(self, other: SafeFilter) -> Self {
        Self {
            expr: self.expr.and(other.expr),
        }
    }

    pub fn or(self, other: SafeFilter) -> Self {
        Self {
            expr: self.expr.or(other.expr),
        }
    }

    /// Build SafeFilter from protobuf MemorySearchFilter.
    /// roles/content_types use OR (a record has one role/content_type).
    /// Other fields use AND.
    pub fn from_proto_filter(
        filter: &protobuf::llm_memory::data::MemorySearchFilter,
    ) -> Option<Self> {
        let mut result: Option<Self> = None;
        let mut combine_and = |f: SafeFilter| {
            result = Some(match result.take() {
                Some(existing) => existing.and(f),
                None => f,
            });
        };

        if let Some(uid) = filter.user_id {
            combine_and(Self::user_id(uid));
        }

        if !filter.roles.is_empty()
            && let Some(role_filter) = filter
                .roles
                .iter()
                .map(|&r| Self::role(r))
                .reduce(|a, b| a.or(b))
        {
            combine_and(role_filter);
        }

        if !filter.content_types.is_empty()
            && let Some(ct_filter) = filter
                .content_types
                .iter()
                .map(|&ct| Self::content_type(ct))
                .reduce(|a, b| a.or(b))
        {
            combine_and(ct_filter);
        }

        if !filter.memory_kinds.is_empty() {
            combine_and(
                Self::memory_kinds_any(&filter.memory_kinds).expect("non-empty memory_kinds"),
            );
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

    /// Convert to LanceDB SQL filter string
    pub fn to_sql(&self) -> anyhow::Result<String> {
        expr_to_safe_string(&self.expr)
    }
}

/// Convert DataFusion Expr to safe SQL string.
/// Follows message-vectordb's expr_to_safe_string() pattern.
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
    fn test_simple_filter() {
        let f = SafeFilter::user_id(42);
        assert_eq!(f.to_sql().unwrap(), "(user_id = 42)");
    }

    #[test]
    fn test_combined_filter() {
        let f = SafeFilter::user_id(1)
            .and(SafeFilter::role(3))
            .and(SafeFilter::created_after(1000));
        let sql = f.to_sql().unwrap();
        assert!(sql.contains("user_id = 1"));
        assert!(sql.contains("role = 3"));
        assert!(sql.contains("created_at > 1000"));
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_or_filter() {
        let f = SafeFilter::role(1).or(SafeFilter::role(2));
        let sql = f.to_sql().unwrap();
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_from_proto_filter() {
        let pf = protobuf::llm_memory::data::MemorySearchFilter {
            user_id: Some(10),
            roles: vec![1, 2],
            content_types: vec![],
            created_after: Some(500),
            created_before: None,
            ..Default::default()
        };
        let sf = SafeFilter::from_proto_filter(&pf).unwrap();
        let sql = sf.to_sql().unwrap();
        assert!(sql.contains("user_id = 10"));
        assert!(sql.contains("role = 1"));
        assert!(sql.contains("role = 2"));
        assert!(sql.contains("OR"));
        assert!(sql.contains("created_at > 500"));
    }

    #[test]
    fn test_updated_after_strict_greater() {
        let f = SafeFilter::updated_after(1000);
        assert_eq!(f.to_sql().unwrap(), "(updated_at > 1000)");
    }

    #[test]
    fn test_updated_before_strict_less() {
        let f = SafeFilter::updated_before(2000);
        assert_eq!(f.to_sql().unwrap(), "(updated_at < 2000)");
    }

    #[test]
    fn test_updated_range_and() {
        let f = SafeFilter::updated_after(100).and(SafeFilter::updated_before(200));
        let sql = f.to_sql().unwrap();
        assert!(sql.contains("updated_at > 100"));
        assert!(sql.contains("updated_at < 200"));
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_from_proto_filter_with_updated_range() {
        // created_* と updated_* を併用したとき AND で結合されること(spec §P4 テスト 3)。
        let pf = protobuf::llm_memory::data::MemorySearchFilter {
            user_id: Some(7),
            created_after: Some(500),
            updated_after: Some(1000),
            updated_before: Some(2000),
            ..Default::default()
        };
        let sf = SafeFilter::from_proto_filter(&pf).unwrap();
        let sql = sf.to_sql().unwrap();
        assert!(sql.contains("user_id = 7"));
        assert!(sql.contains("created_at > 500"));
        assert!(sql.contains("updated_at > 1000"));
        assert!(sql.contains("updated_at < 2000"));
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_from_proto_filter_with_memory_kinds() {
        let pf = protobuf::llm_memory::data::MemorySearchFilter {
            memory_kinds: vec![1, 7],
            ..Default::default()
        };
        let sql = SafeFilter::from_proto_filter(&pf)
            .unwrap()
            .to_sql()
            .unwrap();
        assert_eq!(sql, "(memory_kind IN (1, 7))");
    }

    #[test]
    fn test_sql_injection_prevention() {
        // SafeFilter only accepts typed values, so injection via string is impossible.
        // Scalar values are always parameterized through DataFusion Expr.
        let f = SafeFilter::memory_id(42);
        let sql = f.to_sql().unwrap();
        assert_eq!(sql, "(memory_id = 42)");
    }

    #[test]
    fn test_in_i64_list() {
        let f = SafeFilter::in_i64_list("memory_id", &[1, 2, 3]).unwrap();
        assert_eq!(f.to_sql().unwrap(), "(memory_id IN (1, 2, 3))");
    }

    #[test]
    fn test_in_i64_list_single() {
        let f = SafeFilter::in_i64_list("memory_id", &[42]).unwrap();
        assert_eq!(f.to_sql().unwrap(), "(memory_id IN (42))");
    }

    #[test]
    fn test_in_i64_list_empty() {
        let result = SafeFilter::in_i64_list("memory_id", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_in_i64_list_and_combination() {
        let base = SafeFilter::user_id(10);
        let in_filter = SafeFilter::in_i64_list("memory_id", &[1, 2]).unwrap();
        let combined = base.and(in_filter);
        let sql = combined.to_sql().unwrap();
        assert!(sql.contains("user_id = 10"));
        assert!(sql.contains("memory_id IN (1, 2)"));
        assert!(sql.contains("AND"));
    }

    #[test]
    fn assert_clone() {
        let f = SafeFilter::user_id(1);
        let f2 = f.clone();
        assert_eq!(f.to_sql().unwrap(), f2.to_sql().unwrap());
    }

    #[test]
    fn assert_send_sync() {
        fn _assert<T: Send + Sync + Clone>() {}
        _assert::<SafeFilter>();
    }
}
