use std::fmt;

// ... (весь остальной код файла)

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::Number(s) => write!(f, "{}", s),
            LiteralValue::String(s) => write!(f, "{}", s),
        }
    }
}

use chumsky::prelude::*;

#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    DumpPage(u32),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub values: Vec<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    Text,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub select_list: Vec<SelectItem>,
    pub from_table: Option<String>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectItem {
    /// An expression, e.g., `1`, `1 + 2`, `foo`
    UnnamedExpr(Expression),
    /// `expr AS alias`
    ExprWithAlias { expr: Expression, alias: String },
    /// `*`
    Wildcard,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Literal(LiteralValue),
    Column(String),
    Binary {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum LiteralValue {
    Number(String),
    String(String),
}

pub fn sql_parser(s: &str) -> Result<Vec<Statement>, Vec<Simple<char>>> {
    let ident = text::ident().padded().try_map(|ident: String, span| {
        match ident.to_uppercase().as_str() {
            "SELECT" | "FROM" | "CREATE" | "TABLE" | "INSERT" | "INTO" | "VALUES" | "AS" | "INT" | "TEXT" | "DUMP" | "PAGE" | "UPDATE" | "SET" | "WHERE" | "DELETE" | "ON" | "INDEX" =>
                Err(Simple::custom(span, format!("keyword `{}` cannot be used as an identifier", ident))),
            _ => Ok(ident),
        }
    });

    let number = text::int(10)
        .chain::<char, _, _>(just('.').chain(text::digits(10)).or_not().flatten())
        .collect::<String>()
        .map(LiteralValue::Number);

    let string = just('\'')
        .ignore_then(filter(|c| *c != '\'').repeated())
        .then_ignore(just('\''))
        .collect::<String>()
        .map(LiteralValue::String);

    let literal = number.or(string).map(Expression::Literal).padded();
    let column = ident.clone().map(Expression::Column);

    let expr = recursive(|expr| {
        let atom = literal
            .or(column)
            .or(expr.delimited_by(just('(').padded(), just(')').padded()));

        let op = just("=").to(BinaryOperator::Eq)
            .or(just("<>").to(BinaryOperator::NotEq))
            .or(just("<=").to(BinaryOperator::LtEq))
            .or(just("<").to(BinaryOperator::Lt))
            .or(just(">=").to(BinaryOperator::GtEq))
            .or(just(">").to(BinaryOperator::Gt));

        atom.clone()
            .then(op.padded().then(atom).repeated())
            .foldl(|left, (op, right)| Expression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
    });

    let wildcard = just('*').padded().to(SelectItem::Wildcard);

    let select_item = wildcard.or(expr
        .clone()
        .then(text::keyword("AS").padded().ignore_then(ident.clone()).or_not())
        .map(|(expr, alias)| match alias {
            Some(alias) => SelectItem::ExprWithAlias { expr, alias },
            None => SelectItem::UnnamedExpr(expr),
        }));

    let select = text::keyword("SELECT").padded()
        .ignore_then(
            select_item
                .separated_by(just(',').padded())
                .allow_trailing()
                .collect::<Vec<_>>(),
        )
        .then(text::keyword("FROM").padded().ignore_then(ident.clone()).or_not())
        .then(text::keyword("WHERE").padded().ignore_then(expr.clone()).or_not())
        .map(|((select_list, from_table), where_clause)| {
            Statement::Select(SelectStatement {
                select_list,
                from_table,
                where_clause,
            })
        });

    let data_type = text::ident().try_map(|s: String, span| {
        match s.to_uppercase().as_str() {
            "INT" => Ok(DataType::Int),
            "TEXT" => Ok(DataType::Text),
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
    let commit = text::keyword("COMMIT").padded().to(Statement::Commit);
    let rollback = text::keyword("ROLLBACK").padded().to(Statement::Rollback);

    let statement = create_table
        .or(create_index)
        .or(select)
        .or(insert)
        .or(update)
        .or(delete)
        .or(dump_page)
        .or(begin)
        .or(commit)
        .or(rollback);

    statement
        .padded_by(just(';').repeated())
        .repeated()
        .then_ignore(end())
        .parse(s)
}

