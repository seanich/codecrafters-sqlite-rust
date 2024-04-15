use std::io::{Cursor, Seek, SeekFrom};

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ReadBytesExt};

use crate::db_header::DBHeader;
use crate::schema_object::SchemaObject;
use crate::serial_value::SerialValue;
use crate::ReadVarint;

#[derive(Debug)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl PageType {
    fn from(page_type_byte: u8) -> Result<Self> {
        match page_type_byte {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            _ => bail!("invalid page type"),
        }
    }

    fn is_interior(&self) -> bool {
        match self {
            Self::InteriorIndex | Self::InteriorTable => true,
            Self::LeafIndex | Self::LeafTable => false,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BTreePage {
    page_data: Vec<u8>,

    // Only populated for the first page
    db_header: Option<DBHeader>,

    // Page header content
    page_type: PageType,
    first_freeblock: u16,
    pub num_cells: u16,
    cell_content_start: u16,
    num_fragmented_free_bytes: u8,
    right_most_pointer: Option<u32>,

    pub cell_pointers: Vec<u16>,
}

impl BTreePage {
    pub fn new(data: &[u8], db_header: Option<DBHeader>) -> Result<Self> {
        let mut cursor = Cursor::new(data);
        if let Some(_) = db_header {
            // If this page has the DBHeader skip over it to start reading the page header
            cursor.seek(SeekFrom::Start(DBHeader::SIZE as u64))?;
        }

        let page_type = PageType::from(cursor.read_u8()?)?;
        let first_freeblock = cursor.read_u16::<BigEndian>()?;
        let num_cells = cursor.read_u16::<BigEndian>()?;
        let cell_content_start = cursor.read_u16::<BigEndian>()?;
        let num_fragmented_free_bytes = cursor.read_u8()?;
        let right_most_pointer = if page_type.is_interior() {
            Some(cursor.read_u32::<BigEndian>()?)
        } else {
            None
        };

        let mut cell_pointers = Vec::with_capacity(num_cells as usize);
        for _ in 0..num_cells {
            cell_pointers.push(cursor.read_u16::<BigEndian>()?)
        }

        Ok(Self {
            page_data: data.to_vec(),
            db_header,
            page_type,
            first_freeblock,
            num_cells,
            cell_content_start,
            num_fragmented_free_bytes,
            right_most_pointer,
            cell_pointers,
        })
    }

    pub fn read_cell(&self, data: &[u8]) -> Result<Vec<SerialValue>> {
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

        let mut values = Vec::with_capacity(column_serial_types.len());
        for st in column_serial_types {
            values.push(SerialValue::read(st, &mut reader).context("reading serial value")?)
        }

        Ok(values)
    }

    pub fn read_cells(&self) -> Result<Vec<Vec<SerialValue>>> {
        let mut result = Vec::with_capacity(self.cell_pointers.len());
        for i in 0..self.cell_pointers.len() {
            let cell_data = match i {
                // Cell pointers are in descending order
                0 => &self.page_data[self.cell_pointers[0] as usize..],
                _ => {
                    &self.page_data
                        [self.cell_pointers[i] as usize..self.cell_pointers[i - 1] as usize]
                }
            };
            let cell = self.read_cell(cell_data).context("reading cell data")?;
            result.push(cell);
        }
        Ok(result)
    }

    pub fn load_schemas(&self) -> Result<Vec<SchemaObject>> {
        let mut result = Vec::with_capacity(self.cell_pointers.len());
        let cells = self.read_cells().context("reading schema cells")?;
        for cell in cells {
            result.push(SchemaObject::from(cell).context("construct schema object")?);
        }
        Ok(result)
    }
}
