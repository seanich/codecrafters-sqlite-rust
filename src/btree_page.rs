use std::io::{Cursor, Read, Seek, SeekFrom};

use anyhow::{anyhow, bail, Context, Result};
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
pub enum InteriorCell {
    Table(InteriorTableCell),
    Index(InteriorIndexCell),
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct InteriorTableCell {
    pub left_child_page: u32,
    pub row_id: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct InteriorIndexCell {
    pub left_child_page: u32,
    pub columns: Vec<SerialValue>,
    pub rowid: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BTreePage {
    page_data: Vec<u8>,

    // Only populated for the first page
    db_header: Option<DBHeader>,

    // Page header content
    pub page_type: PageType,
    first_freeblock: u16,
    pub num_cells: u16,
    cell_content_start: u16,
    num_fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,

    pub cell_pointers: Vec<u16>,
}

impl BTreePage {
    pub fn new(data: &[u8], db_header: Option<DBHeader>) -> Result<Self> {
        let mut cursor = Cursor::new(data);
        if db_header.is_some() {
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

    pub fn read_interior_cell(&self, data: &[u8]) -> Result<InteriorCell> {
        let mut reader = Cursor::new(data);
        let left_child_page = reader
            .read_u32::<BigEndian>()
            .context("read left child pointer")?;

        match self.page_type {
            PageType::InteriorTable => {
                let row_id = reader.read_varint().context("read rowid")?;
                Ok(InteriorCell::Table(InteriorTableCell {
                    left_child_page,
                    row_id,
                }))
            }
            PageType::InteriorIndex => {
                let _payload_bytes = reader.read_varint().context("read payload bytes")?;

                let payload = read_payload(&mut reader)?;

                let Some((rowid, columns)) = payload.split_last() else {
                    bail!("interior index cell should have at least two values")
                };
                let Some(rowid) = rowid.as_rowid() else {
                    bail!("last interior index cell value must be a rowid")
                };
                let columns = columns.to_vec();

                Ok(InteriorCell::Index(InteriorIndexCell {
                    left_child_page,
                    rowid,
                    columns,
                }))
            }
            PageType::LeafTable | PageType::LeafIndex => {
                Err(anyhow!("cannot read interior cell from leaf page"))
            }
        }
    }

    pub fn read_interior_cells(&self) -> Result<Vec<InteriorCell>> {
        let num_ptrs = self.cell_pointers.len();
        let mut result = Vec::with_capacity(num_ptrs);
        for &cp in &self.cell_pointers {
            let cell_data = &self.page_data[cp as usize..];
            let cell = self
                .read_interior_cell(cell_data)
                .context("reading cell data")?;
            result.push(cell);
        }
        Ok(result)
    }

    pub fn read_cell(&self, data: &[u8]) -> Result<Vec<SerialValue>> {
        let mut reader = Cursor::new(data);

        let _payload_size = reader.read_varint().context("read payload size")?;

        let row_id = match self.page_type {
            PageType::LeafTable => Some(reader.read_varint().context("read row ID")?),
            _ => None,
        };

        let mut values = read_payload(&mut reader)?;

        // FIXME: This is a terrible hack. I should actually figure out when it's appropriate to
        // substitute the rowid value for the ID column.
        if let SerialValue::Null = values[0] {
            if let Some(id) = row_id {
                values[0] = SerialValue::Int64(id as i64);
            }
        }

        Ok(values)
    }

    pub fn read_cells(&self) -> Result<Vec<Vec<SerialValue>>> {
        let num_ptrs = self.cell_pointers.len();
        let mut result = Vec::with_capacity(num_ptrs);
        for &cp in &self.cell_pointers {
            let cell_data = &self.page_data[cp as usize..];
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

fn read_payload<T>(reader: &mut T) -> Result<Vec<SerialValue>>
where
    T: Read + Seek,
{
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
        values.push(SerialValue::read(st, reader).context("reading serial value")?)
    }

    Ok(values)
}
