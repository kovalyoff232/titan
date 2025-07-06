use chumsky::prelude::*;
use std::fmt;

// --- AST ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement {
    Select(SelectStatement),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStatement {
    pub select_list: Vec<SelectItem>,
    pub from: Vec<TableReference>,
    pub where_clause: Option<Expression>,
    pub order_by: Option<Vec<Expression>>,
    pub for_update: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SelectItem {
    UnnamedExpr(Expression),
    ExprWithAlias {
        expr: Expression,
        alias: String,
    },
    Wildcard,
    QualifiedWildcard(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableReference {
    Table {
        name: String,
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
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd)]
pub enum LiteralValue {
    Number(String),
    String(String),
    Bool(bool),
    Date(String),
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
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::Column(name) => write!(f, "{}", name),
            Expression::QualifiedColumn(table, col) => write!(f, "{}.{}", table, col),
            Expression::Binary { left, op, right } => write!(f, "({} {} {})", left, op, right),
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
        }
    }
}

// --- Parser ---

pub fn sql_parser(s: &str) -> Result<Vec<Statement>, Vec<Simple<char>>> {
    let ident = text::ident().padded().try_map(|ident: String, span| {
        match ident.to_uppercase().as_str() {
            "SELECT" | "FROM" | "CREATE" | "TABLE" | "INSERT" | "INTO" | "VALUES" | "AS" | "INT" | "TEXT" | "BOOLEAN" | "DATE" | "DUMP" | "PAGE" | "UPDATE" | "SET" | "WHERE" | "DELETE" | "ON" | "INDEX" | "JOIN" | "VACUUM" | "START" | "TRANSACTION" | "FOR" | "TRUE" | "FALSE" | "ORDER" | "BY" | "ANALYZE" =>
                Err(Simple::custom(span, format!("keyword `{}` cannot be used as an identifier", ident))),
            _ => Ok(ident),
        }
    });

    let number = text::int(10)
        .chain::<char, _, _>(just('.').chain(text::digits(10)).or_not().flatten())
        .collect::<String>()
        .map(LiteralValue::Number);

    let string_literal = just('\'').ignore_then(filter(|c| *c != '\'').repeated()).then_ignore(just('\'')).collect::<String>();

    let string = string_literal.clone().map(LiteralValue::String);

    let boolean = text::keyword("TRUE").to(true).or(text::keyword("FALSE").to(false)).map(LiteralValue::Bool);

    let date = text::keyword("DATE")
        .padded()
        .ignore_then(string_literal)
        .map(LiteralValue::Date);

    let literal = number.or(string).or(boolean).or(date).map(Expression::Literal).padded();
    
    let qualified_column = ident.clone()
        .then_ignore(just('.'))
        .then(ident.clone())
        .map(|(table, column)| Expression::QualifiedColumn(table, column));

    let column = ident.clone().map(Expression::Column);

    let expr = recursive(|expr| {
        let atom = literal
            .or(qualified_column)
            .or(column)
            .or(expr.delimited_by(just('(').padded(), just(')').padded()));

        let term = atom.clone()
            .then(choice((
                just('+').to(BinaryOperator::Plus),
                just('-').to(BinaryOperator::Minus),
            )).padded().then(atom).repeated())
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });

        let op = just("=").to(BinaryOperator::Eq)
            .or(just("<>").to(BinaryOperator::NotEq))
            .or(just("<=").to(BinaryOperator::LtEq))
            .or(just("<").to(BinaryOperator::Lt))
            .or(just(">=").to(BinaryOperator::GtEq))
            .or(just(">").to(BinaryOperator::Gt));

        term.clone()
            .then(op.padded().then(term).repeated())
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
    });

    let qualified_wildcard = ident.clone().then_ignore(just('.')).then_ignore(just('*')).map(SelectItem::QualifiedWildcard);
    let wildcard = just('*').padded().to(SelectItem::Wildcard);

    let select_item = wildcard.or(qualified_wildcard).or(expr
        .clone()
        .then(text::keyword("AS").padded().ignore_then(ident.clone()).or_not())
        .map(|(expr, alias)| match alias {
            Some(alias) => SelectItem::ExprWithAlias { expr, alias },
            None => SelectItem::UnnamedExpr(expr),
        }));

    let table_reference = recursive(|table_ref| {
        let table = ident.clone().map(|name| TableReference::Table { name });
        
        let join = table.clone()
            .then(
                text::keyword("JOIN").padded()
                .ignore_then(table_ref)
                .then_ignore(text::keyword("ON").padded())
                .then(expr.clone())
                .repeated()
            )
            .foldl(|left, (right, on_condition)| TableReference::Join {
                left: Box::new(left),
                right: Box::new(right),
                on_condition,
            });
        
        join
    });

    let select = text::keyword("SELECT").padded()
        .ignore_then(
            select_item
                .separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>()
        )
        .then(text::keyword("FROM").padded().ignore_then(
            table_reference.separated_by(just(',').padded()).collect()
        ).or_not())
        .then(text::keyword("WHERE").padded().ignore_then(expr.clone()).or_not())
        .then(text::keyword("ORDER").padded().ignore_then(text::keyword("BY")).padded().ignore_then(expr.clone().separated_by(just(',').padded()).collect()).or_not())
        .then(text::keyword("FOR").padded().ignore_then(text::keyword("UPDATE")).padded().or_not())
        .map(|((((select_list, from), where_clause), order_by), for_update)| {
            Statement::Select(SelectStatement {
                select_list,
                from: from.unwrap_or_default(),
                where_clause,
                order_by,
                for_update: for_update.is_some(),
            })
        });
    
    let data_type = text::ident().try_map(|s: String, span| {
        match s.to_uppercase().as_str() {
            "INT" => Ok(DataType::Int),
            "TEXT" => Ok(DataType::Text),
            "BOOLEAN" => Ok(DataType::Bool),
            "DATE" => Ok(DataType::Date),
            _ => Err(Simple::custom(span, format!("unknown type: {}", s))),
        }
    }).padded();


    let column_def = ident.clone()
        .then(data_type)
        .map(|(name, data_type)| ColumnDef { name, data_type });

    let create_table = text::keyword("CREATE").padded()
        .ignore_then(text::keyword("TABLE").padded())
        .ignore_then(ident.clone())
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
        
    let create_index = text::keyword("CREATE").padded()
        .ignore_then(text::keyword("INDEX").padded())
        .ignore_then(ident.clone())
        .then_ignore(text::keyword("ON").padded())
        .then(ident.clone())
        .then(ident.clone().delimited_by(just('('), just(')')))
        .map(|((index_name, table_name), column_name)| {
            Statement::CreateIndex(CreateIndexStatement {
                index_name,
                table_name,
                column_name,
            })
        });

    let insert = text::keyword("INSERT").padded()
        .ignore_then(text::keyword("INTO").padded())
        .ignore_then(ident.clone())
        .then_ignore(text::keyword("VALUES").padded())
        .then(
            expr.clone().separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>()
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .map(|(table_name, values)| {
            Statement::Insert(InsertStatement {
                table_name,
                values,
            })
        });

    let update = text::keyword("UPDATE").padded()
        .ignore_then(ident.clone())
        .then_ignore(text::keyword("SET").padded())
        .then(
            ident.clone()
                .then_ignore(just('=').padded())
                .then(expr.clone())
                .separated_by(just(',').padded())
                .at_least(1)
                .collect::<Vec<(String, Expression)>>(),
        )
        .then(text::keyword("WHERE").padded().ignore_then(expr.clone()).or_not())
        .map(|((table_name, assignments), where_clause)| {
            Statement::Update(UpdateStatement {
                table_name,
                assignments,
                where_clause,
            })
        });

    let delete = text::keyword("DELETE").padded()
        .ignore_then(text::keyword("FROM").padded())
        .ignore_then(ident.clone())
        .then(text::keyword("WHERE").padded().ignore_then(expr.clone()).or_not())
        .map(|(table_name, where_clause)| {
            Statement::Delete(DeleteStatement { table_name, where_clause })
        });

    let dump_page = text::keyword("DUMP").padded()
        .ignore_then(text::keyword("PAGE").padded())
        .ignore_then(text::int(10).map(|s: String| s.parse().unwrap()))
        .map(Statement::DumpPage);

    let begin = text::keyword("BEGIN").padded().to(Statement::Begin);
    let start_transaction = text::keyword("START").padded().then(text::keyword("TRANSACTION")).padded().to(Statement::Begin);
    let commit = text::keyword("COMMIT").padded().to(Statement::Commit);
    let rollback = text::keyword("ROLLBACK").padded().to(Statement::Rollback);

    let vacuum = text::keyword("VACUUM").padded()
        .ignore_then(ident.clone())
        .map(Statement::Vacuum);

    let analyze = text::keyword("ANALYZE").padded()
        .ignore_then(ident.clone())
        .map(Statement::Analyze);

    let statement = create_table
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
        .or(analyze);

    statement
        .padded_by(just(';').repeated())
        .repeated()
        .then_ignore(end())
        .parse(s)
}