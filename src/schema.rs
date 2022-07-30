use crate::record::ColumnValue;

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    pub name: String,
    pub table_name: String,
    pub root_page: u8,
    pub sql: String,
}

impl Schema {
    /// Parses a record into a schema
    pub fn parse(record: Vec<ColumnValue>) -> Option<Self> {
        let mut items = record.into_iter();
        let kind = items.next()?.read_string();
        let name = items.next()?.read_string();
        let table_name = items.next()?.read_string();
        let root_page = items.next()?.read_u8();
        let sql = items.next()?.read_string();

        let schema = Self {
            kind,
            name,
            table_name,
            root_page,
            sql,
        };
        Some(schema)
    }
}
