use std::io::{Cursor, Seek};

use anyhow::{bail, Context, Result};

use sqlite_starter_rust::ReadVarint;

use crate::serial_value::SerialValue;

#[derive(Debug)]
enum ObjectType {
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

#[derive(Debug)]
#[allow(dead_code)]
pub struct SchemaObject {
    object_type: ObjectType,
    name: String,
    pub table_name: String,
    root_page: Option<usize>,
    sql: String,
}

impl SchemaObject {
    pub fn from(data: &[u8]) -> Result<Self> {
        let mut reader = Cursor::new(data);

        let _payload_size = reader.read_varint().context("read payload size")?;
        let _row_id = reader.read_varint().context("read row ID")?;

        let header_start = reader.stream_position()?;
        let header_size = reader.read_varint().context("read header size")?;

        // Encoded as serial types https://www.sqlite.org/fileformat.html#record_format
        let mut column_serial_types = Vec::new();
        while reader.stream_position()? < header_start + header_size {
            let column_type = reader
                .read_varint()
                .context("read column serial type varint")?;
            column_serial_types.push(column_type);
        }
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
            column_serial_types.len(),
            5,
            "should have exactly 5 columns for schema table"
        );

        let object_type = match SerialValue::read(column_serial_types[0], &mut reader)
            .context("reading object_type value")?
        {
            SerialValue::Text(value) => ObjectType::from(&value)?,
            _ => bail!("unexpected serial value for object_type"),
        };

        let name = match SerialValue::read(column_serial_types[1], &mut reader)
            .context("reading name value")?
        {
            SerialValue::Text(value) => value,
            _ => bail!("unexpected serial value for name"),
        };

        let table_name = match SerialValue::read(column_serial_types[2], &mut reader)
            .context("reading table_name value")?
        {
            SerialValue::Text(value) => value,
            _ => bail!("unexpected serial value for table_name"),
        };

        let root_page = match SerialValue::read(column_serial_types[3], &mut reader)
            .context("reading root_page value")?
        {
            SerialValue::Null => None,
            SerialValue::Int8(value) => Some(value as usize),
            _ => bail!("unexpected serial value for root_page"),
        };

        let sql = match SerialValue::read(column_serial_types[4], &mut reader)
            .context("reading sql value")?
        {
            SerialValue::Text(value) => value,
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
}
