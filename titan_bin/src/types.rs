#[derive(Debug)]
pub enum ExecuteResult {
    ResultSet(ResultSet),
    Insert(u32),
    Delete(u32),
    Update(u32),
    Ddl,
}

#[derive(Clone, Debug)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}
pub type Row = Vec<String>;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub type_id: u32,
}
