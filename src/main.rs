use std::fs::File;

use anyhow::{bail, Context, Result};

use crate::db_file::DBFile;

mod btree_page;
mod db_file;
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
