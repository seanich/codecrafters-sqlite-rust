use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use anyhow::{bail, Context, Result};

use crate::btree_page::BTreePage;
use crate::db_header::DBHeader;

mod btree_page;
mod db_header;
mod macros;
mod schema_object;
mod serial_value;

const SQLITE_TABLE_PREFIX: &str = "sqlite_";

struct DBFile<'a> {
    file: &'a mut File,

    header: DBHeader,
    first_page: BTreePage,
}

impl<'a> DBFile<'a> {
    pub fn new(file: &'a mut File) -> Result<Self> {
        let mut header = [0; DBHeader::SIZE];
        file.read_exact(&mut header)?;
        let db_header = DBHeader::from_bytes(&header).expect("should parse header");

        // Seek back to the start of the file
        file.seek(SeekFrom::Start(0))?;

        let mut page = vec![0u8; db_header.page_size() as usize];
        file.read_exact(&mut page)?;
        let page = BTreePage::new(&page, Some(db_header)).expect("should construct BTree page");

        return Ok(Self {
            file,
            header: db_header,
            first_page: page,
        });
    }

    pub(crate) fn row_count(&mut self, table_name: &str) -> Result<usize> {
        let page_size = self.header.page_size() as u64;

        // Find the table schema
        let table_schema = match self
            .first_page
            .load_schemas()
            .context("loading schemas")?
            .into_iter()
            .find(|s| s.table_name == table_name)
        {
            Some(s) => s,
            None => bail!("could not find table with name '{}'", table_name),
        };

        // Seek to page start
        let page_offset = table_schema.root_page.context("getting root page offset")? - 1;
        self.file
            .seek(SeekFrom::Start(page_offset as u64 * page_size))
            .context("seeking to root page offset")?;

        // Load root page for table
        let mut buf = vec![0u8; page_size as usize];
        self.file
            .read_exact(&mut buf)
            .context("reading root page for table")?;
        let page = BTreePage::new(&buf, None).context("building BTree page")?;

        // Get number of cells (i.e. row count)
        Ok(page.num_cells as usize)
    }
}

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
            let db_file = DBFile::new(&mut file).context("constructing DBFile")?;

            println!("database page size: {}", db_file.header.page_size());
            println!("number of tables: {}", db_file.first_page.num_cells);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let db_file = DBFile::new(&mut file).context("constructing DBFile")?;

            let mut table_names = vec![];
            for schema_obj in db_file.first_page.load_schemas().context("load schemas")? {
                if !schema_obj.table_name.starts_with(SQLITE_TABLE_PREFIX) {
                    table_names.push(schema_obj.table_name);
                }
            }
            println!("{}", table_names.join(" "));
        }
        command => {
            let (_, table_name) = command
                .rsplit_once(" ")
                .context("expected table name at end of command")?;

            let mut file = File::open(&args[1])?;
            let mut db_file = DBFile::new(&mut file).context("constructing DBFile")?;

            let row_count = db_file
                .row_count(table_name)
                .context("finding row count for table")?;
            println!("{}", row_count);
        }
    }

    Ok(())
}
