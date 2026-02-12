use chumsky::prelude::*;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    Select(Box<SelectStatement>),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Vacuum(String),
    Analyze(String),
    Begin,
    Commit,
    Rollback,
    DumpPage(u32),
    Explain(Box<Statement>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStatement {
    pub with_clause: Option<Vec<CommonTableExpression>>,
    pub select_list: Vec<SelectItem>,
    pub from: Vec<TableReference>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub for_update: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    UnnamedExpr(Expression),
    ExprWithAlias { expr: Expression, alias: String },
    Wildcard,
    QualifiedWildcard(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableReference {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on_condition: Expression,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int,
    Text,
    Bool,
    Date,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expression {
    Literal(LiteralValue),
    Column(String),
    QualifiedColumn(String, String),
    Binary {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    WindowFunction {
        function: WindowFunctionType,
        args: Vec<Expression>,
        over: WindowSpec,
    },
    Function {
        name: String,
        args: Vec<Expression>,
    },
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LiteralValue {
    Number(String),
    String(String),
    Bool(bool),
    Date(String),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr {
    pub expr: Expression,
    pub asc: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrame {
    Rows(FrameBound, FrameBound),
    Range(FrameBound, FrameBound),
    Groups(FrameBound, FrameBound),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile(i32),
    Lag {
        offset: i64,
        default: Option<Box<Expression>>,
    },
    Lead {
        offset: i64,
        default: Option<Box<Expression>>,
    },
    FirstValue,
    LastValue,
    NthValue(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommonTableExpression {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStatement>,
    pub recursive: bool,
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::Column(name) => write!(f, "{}", name),
            Expression::QualifiedColumn(table, col) => write!(f, "{}.{}", table, col),
            Expression::Binary { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expression::Unary { op, expr } => write!(f, "({} {})", op, expr),
            Expression::WindowFunction { function, .. } => write!(
                f,
                "{}() OVER (...)",
                match function {
                    WindowFunctionType::RowNumber => "ROW_NUMBER",
                    WindowFunctionType::Rank => "RANK",
                    WindowFunctionType::DenseRank => "DENSE_RANK",
                    WindowFunctionType::PercentRank => "PERCENT_RANK",
                    WindowFunctionType::CumeDist => "CUME_DIST",
                    WindowFunctionType::Ntile(_) => "NTILE",
                    WindowFunctionType::Lag { .. } => "LAG",
                    WindowFunctionType::Lead { .. } => "LEAD",
                    WindowFunctionType::FirstValue => "FIRST_VALUE",
                    WindowFunctionType::LastValue => "LAST_VALUE",
                    WindowFunctionType::NthValue(_) => "NTH_VALUE",
                }
            ),
            Expression::Function { name, args } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            Expression::Case { .. } => write!(f, "CASE ... END"),
            Expression::Subquery(_) => write!(f, "(SELECT ...)"),
        }
    }
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Number(s) => write!(f, "{}", s),
            LiteralValue::String(s) => write!(f, "{}", s),
            LiteralValue::Bool(b) => write!(f, "{}", b),
            LiteralValue::Date(s) => write!(f, "{}", s),
            LiteralValue::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperator::Not => write!(f, "NOT"),
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::NotEq => write!(f, "<>"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::LtEq => write!(f, "<="),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::GtEq => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
        }
    }
}

pub fn sql_parser(s: &str) -> Result<Vec<Statement>, Vec<Simple<char>>> {
    let ident =
        text::ident()
            .padded()
            .try_map(|ident: String, span| match ident.to_uppercase().as_str() {
                "SELECT" | "FROM" | "CREATE" | "TABLE" | "INSERT" | "INTO" | "VALUES" | "AS"
                | "INT" | "TEXT" | "BOOLEAN" | "DATE" | "DUMP" | "PAGE" | "UPDATE" | "SET"
                | "WHERE" | "DELETE" | "ON" | "INDEX" | "JOIN" | "VACUUM" | "START"
                | "TRANSACTION" | "FOR" | "TRUE" | "FALSE" | "ORDER" | "BY" | "ANALYZE"
                | "GROUP" | "HAVING" | "EXPLAIN" | "NOT" | "OR" | "ASC" | "DESC" | "LIMIT"
                | "OFFSET" | "NULL" | "NULLS" | "FIRST" | "LAST" => Err(Simple::custom(
                    span,
                    format!("keyword `{}` cannot be used as an identifier", ident),
                )),
                _ => Ok(ident),
            });

    let number = text::int(10)
        .chain::<char, _, _>(just('.').chain(text::digits(10)).or_not().flatten())
        .collect::<String>()
        .map(LiteralValue::Number);

    let string_literal = just('\'')
        .ignore_then(filter(|c| *c != '\'').repeated())
        .then_ignore(just('\''))
        .collect::<String>();

    let string = string_literal.map(LiteralValue::String);

    let boolean = text::keyword("TRUE")
        .to(true)
        .or(text::keyword("FALSE").to(false))
        .map(LiteralValue::Bool);

    let null = text::keyword("NULL").to(LiteralValue::Null);

    let date = text::keyword("DATE")
        .padded()
        .ignore_then(string_literal)
        .map(LiteralValue::Date);

    let literal = number
        .or(string)
        .or(boolean)
        .or(date)
        .or(null)
        .map(Expression::Literal)
        .padded();

    let qualified_column = ident
        .then_ignore(just('.'))
        .then(ident)
        .map(|(table, column)| Expression::QualifiedColumn(table, column));

    let column = ident.map(Expression::Column);

    let expr = recursive(|expr| {
        let function_call = ident
            .then(
                just('(')
                    .padded()
                    .ignore_then(
                        just('*').padded().to(Vec::<Expression>::new()).or(expr
                            .clone()
                            .separated_by(just(',').padded())
                            .allow_trailing()
                            .collect::<Vec<_>>()),
                    )
                    .then_ignore(just(')').padded()),
            )
            .map(|(name, args)| Expression::Function { name, args });

        let atom = literal
            .or(function_call)
            .or(qualified_column)
            .or(column)
            .or(expr
                .clone()
                .delimited_by(just('(').padded(), just(')').padded()));

        let unary = text::keyword("NOT")
            .padded()
            .map(|_| UnaryOperator::Not)
            .then(atom.clone())
            .map(|(op, expr)| Expression::Unary {
                op,
                expr: Box::new(expr),
            })
            .or(atom);

        let product = unary
            .clone()
            .then(
                choice((
                    just('+').to(BinaryOperator::Plus),
                    just('-').to(BinaryOperator::Minus),
                ))
                .padded()
                .then(unary)
                .repeated(),
            )
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });

        let comparison_op = just("=")
            .to(BinaryOperator::Eq)
            .or(just("!=").to(BinaryOperator::NotEq))
            .or(just("<>").to(BinaryOperator::NotEq))
            .or(just("<=").to(BinaryOperator::LtEq))
            .or(just("<").to(BinaryOperator::Lt))
            .or(just(">=").to(BinaryOperator::GtEq))
            .or(just(">").to(BinaryOperator::Gt));

        let comparison = product
            .clone()
            .then(comparison_op.padded().then(product).repeated())
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });

        let conjunction = comparison
            .clone()
            .then(
                text::keyword("AND")
                    .padded()
                    .to(BinaryOperator::And)
                    .then(comparison)
                    .repeated(),
            )
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });

        conjunction
            .clone()
            .then(
                text::keyword("OR")
                    .padded()
                    .to(BinaryOperator::Or)
                    .then(conjunction)
                    .repeated(),
            )
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
    });

    let qualified_wildcard = ident
        .then_ignore(just('.'))
        .then_ignore(just('*'))
        .map(SelectItem::QualifiedWildcard);
    let wildcard = just('*').padded().to(SelectItem::Wildcard);

    let select_item = wildcard.or(qualified_wildcard).or(expr
        .clone()
        .then(text::keyword("AS").padded().ignore_then(ident).or_not())
        .map(|(expr, alias)| match alias {
            Some(alias) => SelectItem::ExprWithAlias { expr, alias },
            None => SelectItem::UnnamedExpr(expr),
        }));

    let table_reference = recursive(|table_ref| {
        let table_alias = text::keyword("AS")
            .padded()
            .ignore_then(ident)
            .or(ident)
            .or_not();

        let table = ident
            .then(table_alias)
            .map(|(name, alias)| TableReference::Table { name, alias });

        table
            .then(
                text::keyword("JOIN")
                    .padded()
                    .ignore_then(table_ref)
                    .then_ignore(text::keyword("ON").padded())
                    .then(expr.clone())
                    .repeated(),
            )
            .foldl(|left, (right, on_condition)| TableReference::Join {
                left: Box::new(left),
                right: Box::new(right),
                on_condition,
            })
    });

    let select = text::keyword("SELECT")
        .padded()
        .ignore_then(
            select_item
                .separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>(),
        )
        .then(
            text::keyword("FROM")
                .padded()
                .ignore_then(table_reference.separated_by(just(',').padded()).collect())
                .or_not(),
        )
        .then(
            text::keyword("WHERE")
                .padded()
                .ignore_then(expr.clone())
                .or_not(),
        )
        .then(
            text::keyword("GROUP")
                .padded()
                .ignore_then(text::keyword("BY").padded())
                .ignore_then(expr.clone().separated_by(just(',').padded()).collect())
                .or_not(),
        )
        .then(
            text::keyword("HAVING")
                .padded()
                .ignore_then(expr.clone())
                .or_not(),
        )
        .then({
            let order_by_expr = expr
                .clone()
                .then(
                    text::keyword("ASC")
                        .padded()
                        .to(true)
                        .or(text::keyword("DESC").padded().to(false))
                        .or_not(),
                )
                .then(
                    text::keyword("NULLS")
                        .padded()
                        .ignore_then(
                            text::keyword("FIRST")
                                .padded()
                                .to(true)
                                .or(text::keyword("LAST").padded().to(false)),
                        )
                        .or_not(),
                )
                .map(|((expr, asc), nulls_first)| OrderByExpr {
                    expr,
                    asc: asc.unwrap_or(true),
                    nulls_first,
                });

            text::keyword("ORDER")
                .padded()
                .ignore_then(text::keyword("BY"))
                .padded()
                .ignore_then(order_by_expr.separated_by(just(',').padded()).collect())
                .or_not()
        })
        .then(
            text::keyword("LIMIT")
                .padded()
                .ignore_then(text::int(10).padded().try_map(|value: String, span| {
                    value
                        .parse::<i64>()
                        .map_err(|_| Simple::custom(span, format!("invalid LIMIT value: {value}")))
                }))
                .or_not(),
        )
        .then(
            text::keyword("OFFSET")
                .padded()
                .ignore_then(text::int(10).padded().try_map(|value: String, span| {
                    value
                        .parse::<i64>()
                        .map_err(|_| Simple::custom(span, format!("invalid OFFSET value: {value}")))
                }))
                .or_not(),
        )
        .then(
            text::keyword("FOR")
                .padded()
                .ignore_then(text::keyword("UPDATE"))
                .padded()
                .or_not(),
        )
        .map(
            |(
                (
                    ((((((select_list, from), where_clause), group_by), having), order_by), limit),
                    offset,
                ),
                for_update,
            )| {
                Statement::Select(Box::new(SelectStatement {
                    with_clause: None,
                    select_list,
                    from: from.unwrap_or_default(),
                    where_clause,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                    for_update: for_update.is_some(),
                }))
            },
        );

    let data_type = text::ident()
        .try_map(|s: String, span| match s.to_uppercase().as_str() {
            "INT" => Ok(DataType::Int),
            "TEXT" => Ok(DataType::Text),
            "BOOLEAN" => Ok(DataType::Bool),
            "DATE" => Ok(DataType::Date),
            _ => Err(Simple::custom(span, format!("unknown type: {}", s))),
        })
        .padded();

    let column_def = ident
        .then(data_type)
        .map(|(name, data_type)| ColumnDef { name, data_type });

    let create_table = text::keyword("CREATE")
        .padded()
        .ignore_then(text::keyword("TABLE").padded())
        .ignore_then(ident)
        .then(
            column_def
                .separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .map(|(table_name, columns)| {
            Statement::CreateTable(CreateTableStatement {
                table_name,
                columns,
            })
        });

    let create_index = text::keyword("CREATE")
        .padded()
        .ignore_then(text::keyword("INDEX").padded())
        .ignore_then(ident)
        .then_ignore(text::keyword("ON").padded())
        .then(ident)
        .then(ident.delimited_by(just('('), just(')')))
        .map(|((index_name, table_name), column_name)| {
            Statement::CreateIndex(CreateIndexStatement {
                index_name,
                table_name,
                column_name,
            })
        });

    let insert = text::keyword("INSERT")
        .padded()
        .ignore_then(text::keyword("INTO").padded())
        .ignore_then(ident)
        .then_ignore(text::keyword("VALUES").padded())
        .then(
            expr.clone()
                .separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .map(|(table_name, values)| Statement::Insert(InsertStatement { table_name, values }));

    let update = text::keyword("UPDATE")
        .padded()
        .ignore_then(ident)
        .then_ignore(text::keyword("SET").padded())
        .then(
            ident
                .then_ignore(just('=').padded())
                .then(expr.clone())
                .separated_by(just(',').padded())
                .at_least(1)
                .collect::<Vec<(String, Expression)>>(),
        )
        .then(
            text::keyword("WHERE")
                .padded()
                .ignore_then(expr.clone())
                .or_not(),
        )
        .map(|((table_name, assignments), where_clause)| {
            Statement::Update(UpdateStatement {
                table_name,
                assignments,
                where_clause,
            })
        });

    let delete = text::keyword("DELETE")
        .padded()
        .ignore_then(text::keyword("FROM").padded())
        .ignore_then(ident)
        .then(
            text::keyword("WHERE")
                .padded()
                .ignore_then(expr.clone())
                .or_not(),
        )
        .map(|(table_name, where_clause)| {
            Statement::Delete(DeleteStatement {
                table_name,
                where_clause,
            })
        });

    let dump_page = text::keyword("DUMP")
        .padded()
        .ignore_then(text::keyword("PAGE").padded())
        .ignore_then(text::int(10).try_map(|s: String, span| {
            s.parse::<u32>()
                .map_err(|_| Simple::custom(span, format!("invalid page id: {}", s)))
        }))
        .map(Statement::DumpPage);

    let begin = text::keyword("BEGIN").padded().to(Statement::Begin);
    let start_transaction = text::keyword("START")
        .padded()
        .then(text::keyword("TRANSACTION"))
        .padded()
        .to(Statement::Begin);
    let commit = text::keyword("COMMIT").padded().to(Statement::Commit);
    let rollback = text::keyword("ROLLBACK").padded().to(Statement::Rollback);

    let vacuum = text::keyword("VACUUM")
        .padded()
        .ignore_then(ident)
        .map(Statement::Vacuum);

    let analyze = text::keyword("ANALYZE")
        .padded()
        .ignore_then(ident)
        .map(Statement::Analyze);

    let statement = recursive(|statement| {
        let explain = text::keyword("EXPLAIN")
            .padded()
            .ignore_then(statement)
            .map(|stmt| Statement::Explain(Box::new(stmt)));

        create_table
            .or(create_index)
            .or(select)
            .or(insert)
            .or(update)
            .or(delete)
            .or(dump_page)
            .or(begin)
            .or(start_transaction)
            .or(commit)
            .or(rollback)
            .or(vacuum)
            .or(analyze)
            .or(explain)
    });

    statement
        .padded_by(just(';').repeated())
        .repeated()
        .then_ignore(end())
        .parse(s)
}

#[cfg(test)]
mod tests {
    use super::{Expression, Statement, TableReference, sql_parser};

    #[test]
    fn dump_page_parses_valid_u32() {
        let parsed = sql_parser("DUMP PAGE 42;").expect("DUMP PAGE should parse");
        assert_eq!(parsed, vec![Statement::DumpPage(42)]);
    }

    #[test]
    fn dump_page_rejects_out_of_range_value() {
        let parsed = sql_parser("DUMP PAGE 4294967296;");
        assert!(parsed.is_err());
    }

    #[test]
    fn select_order_by_parses_direction_modifiers() {
        let parsed =
            sql_parser("SELECT id, name FROM users ORDER BY id DESC, name ASC;").expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        let order_by = stmt.order_by.as_ref().expect("ORDER BY must be present");
        assert_eq!(order_by.len(), 2);
        assert_eq!(order_by[0].expr, Expression::Column("id".to_string()));
        assert!(!order_by[0].asc);
        assert_eq!(order_by[1].expr, Expression::Column("name".to_string()));
        assert!(order_by[1].asc);
    }

    #[test]
    fn select_order_by_parses_nulls_modifiers() {
        let parsed =
            sql_parser("SELECT id, name FROM users ORDER BY id DESC NULLS LAST, name NULLS FIRST;")
                .expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        let order_by = stmt.order_by.as_ref().expect("ORDER BY must be present");
        assert_eq!(order_by.len(), 2);
        assert_eq!(order_by[0].expr, Expression::Column("id".to_string()));
        assert!(!order_by[0].asc);
        assert_eq!(order_by[0].nulls_first, Some(false));
        assert_eq!(order_by[1].expr, Expression::Column("name".to_string()));
        assert!(order_by[1].asc);
        assert_eq!(order_by[1].nulls_first, Some(true));
    }

    #[test]
    fn select_limit_and_offset_are_parsed() {
        let parsed =
            sql_parser("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1;").expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        assert_eq!(stmt.limit, Some(2));
        assert_eq!(stmt.offset, Some(1));
    }

    #[test]
    fn select_where_parses_bang_not_equal_operator() {
        let parsed = sql_parser("SELECT id FROM users WHERE name != 'Bob';").expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        let where_expr = stmt.where_clause.as_ref().expect("WHERE must be present");
        let Expression::Binary { op, .. } = where_expr else {
            panic!("expected binary expression in WHERE");
        };
        assert_eq!(*op, super::BinaryOperator::NotEq);
    }

    #[test]
    fn select_null_literal_is_parsed() {
        let parsed = sql_parser("SELECT NULL;").expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        assert!(matches!(
            stmt.select_list[0],
            super::SelectItem::UnnamedExpr(Expression::Literal(super::LiteralValue::Null))
        ));
    }

    #[test]
    fn select_group_by_and_having_are_parsed() {
        let parsed = sql_parser(
            "SELECT group_id, COUNT(*) AS cnt FROM users GROUP BY group_id HAVING cnt > 1;",
        )
        .expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        assert!(stmt.group_by.is_some());
        assert!(stmt.having.is_some());
    }

    #[test]
    fn select_from_table_aliases_are_parsed() {
        let parsed =
            sql_parser("SELECT u.id, o.item FROM users u JOIN orders AS o ON u.id = o.user_id;")
                .expect("parse");
        let Statement::Select(stmt) = &parsed[0] else {
            panic!("expected SELECT statement");
        };
        assert_eq!(stmt.from.len(), 1);
        let TableReference::Join { left, right, .. } = &stmt.from[0] else {
            panic!("expected JOIN table reference");
        };
        let TableReference::Table { name, alias } = left.as_ref() else {
            panic!("expected left table");
        };
        assert_eq!(name, "users");
        assert_eq!(alias.as_deref(), Some("u"));
        let TableReference::Table { name, alias } = right.as_ref() else {
            panic!("expected right table");
        };
        assert_eq!(name, "orders");
        assert_eq!(alias.as_deref(), Some("o"));
    }
}
