use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use anyhow::{bail, Context, Result};

use crate::btree_page::BTreePage;
use crate::db_header::DBHeader;
use crate::schema_object::SchemaObject;

mod btree_page;
mod db_header;
mod macros;
mod schema_object;
mod serial_value;

const SQLITE_TABLE_PREFIX: &str = "sqlite_";

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; DBHeader::SIZE];
            file.read_exact(&mut header)?;
            let db_header = DBHeader::from_bytes(&header).expect("should parse header");
            println!("database page size: {}", db_header.page_size());

            // Seek back to the start of the file
            file.seek(SeekFrom::Start(0))?;

            let mut page = vec![0u8; db_header.page_size() as usize];
            file.read_exact(&mut page)?;
            let page = BTreePage::new(&page, Some(db_header)).expect("should construct BTree page");
            println!("number of tables: {}", page.num_cells);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; DBHeader::SIZE];
            file.read_exact(&mut header)?;
            let db_header = DBHeader::from_bytes(&header).expect("should parse header");

            // Seek back to the start of the file
            file.seek(SeekFrom::Start(0))?;

            let mut page_buf = vec![0u8; db_header.page_size() as usize];
            file.read_exact(&mut page_buf)?;
            let page =
                BTreePage::new(&page_buf, Some(db_header)).expect("should construct BTree page");

            let mut table_names = Vec::with_capacity(page.num_cells as usize);
            for i in 0..page.cell_pointers.len() {
                let cell_data = match i {
                    // Cell pointers are in descending order
                    0 => &page_buf[page.cell_pointers[0] as usize..],
                    _ => {
                        &page_buf
                            [page.cell_pointers[i] as usize..page.cell_pointers[i - 1] as usize]
                    }
                };
                let schema_obj =
                    SchemaObject::from(cell_data).context("construct schema object")?;

                if !schema_obj.table_name.starts_with(SQLITE_TABLE_PREFIX) {
                    table_names.push(schema_obj.table_name);
                }
            }
            println!("{}", table_names.join(" "));
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
