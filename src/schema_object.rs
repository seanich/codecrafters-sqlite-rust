use anyhow::{bail, Context, Result};
use std::collections::HashMap;

use crate::serial_value::SerialValue;
use crate::sql::sql::sql_statement;
use crate::sql::Statement;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ObjectType {
    Table,
    Index,
    View,
    Trigger,
}

impl ObjectType {
    fn from(type_str: &str) -> Result<Self> {
        match type_str {
            "table" => Ok(Self::Table),
            "index" => Ok(Self::Index),
            "view" => Ok(Self::View),
            "trigger" => Ok(Self::Trigger),
            _ => bail!("unknown object type '{}'", type_str),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SchemaObject {
    pub object_type: ObjectType,
    pub name: String,
    pub table_name: String,
    pub root_page: Option<usize>,
    pub sql: String,
}

impl SchemaObject {
    pub fn from(cell: Vec<SerialValue>) -> Result<Self> {
        // We're only handling the schema table here. Columns should be ordered as follows:
        //
        //      CREATE TABLE sqlite_schema(
        //        type text,
        //        name text,
        //        tbl_name text,
        //        rootpage integer,
        //        sql text
        //      );
        //
        // https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
        assert_eq!(
            cell.len(),
            5,
            "should have exactly 5 columns for schema table"
        );

        let object_type = match &cell[0] {
            SerialValue::Text(value) => ObjectType::from(value)?,
            _ => bail!("unexpected serial value for object_type"),
        };

        let name = match &cell[1] {
            SerialValue::Text(value) => value.to_string(),
            _ => bail!("unexpected serial value for name"),
        };

        let table_name = match &cell[2] {
            SerialValue::Text(value) => value.to_string(),
            _ => bail!("unexpected serial value for table_name"),
        };

        let root_page = match &cell[3] {
            SerialValue::Null => None,
            SerialValue::Int8(value) => Some(*value as usize),
            v => match v.as_usize() {
                Some(u) => Some(u),
                None => bail!("unexpected serial value for root_page: {:?}", v),
            },
        };

        let sql = match &cell[4] {
            SerialValue::Text(value) => value.to_string(),
            _ => bail!("unexpected serial value for sql"),
        };

        Ok(Self {
            object_type,
            name,
            table_name,
            root_page,
            sql,
        })
    }

    pub fn column_order(&self) -> Result<Vec<String>> {
        match sql_statement(&self.sql).context("parsing create table statement")? {
            Statement::CreateTable(create_statement) => Ok(create_statement.columns),
            _ => bail!("invalid create statement"),
        }
    }

    pub fn column_map(&self) -> Result<HashMap<String, usize>> {
        Ok(self
            .column_order()
            .context("retrieving column order")?
            .iter()
            .enumerate()
            .map(|(ind, col)| (col.clone(), ind))
            .collect::<HashMap<_, _>>())
    }
}
