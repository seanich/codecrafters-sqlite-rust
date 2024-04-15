use std::fs::File;

use anyhow::{bail, Context, Ok, Result};
use itertools::Itertools;
use sqlite_starter_rust::btree_page::{BTreePage, PageType};
use sqlite_starter_rust::db_file::DBFile;
use sqlite_starter_rust::schema_object::SchemaObject;
use sqlite_starter_rust::sql::sql::sql_statement;
use sqlite_starter_rust::sql::{SelectStatement, Statement};

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
            let statement = sql_statement(command).context("parsing SQL statement")?;
            match statement {
                Statement::Select(s) => {
                    let mut file = File::open(&args[1])?;
                    let mut db_file = DBFile::new(&mut file).context("constructing DBFile")?;

                    if s.select.len() == 1 && s.select[0].eq_ignore_ascii_case("count(*)") {
                        // Count
                        let row_count = db_file
                            .row_count(&s.from)
                            .context("finding row count for table")?;
                        println!("{}", row_count);
                    } else {
                        // Regular select
                        let schema = db_file
                            .schema_for_table(&s.from)
                            .context("loading table schema")?;

                        let root_page = db_file
                            .load_page_at(
                                schema
                                    .root_page
                                    .context("getting root page from table schema")?,
                            )
                            .context("loading root page for table")?;

                        return execute_select(&mut db_file, &schema, root_page, &s);
                    }
                }
                Statement::Create(_) => bail!("create statements not supported"),
            }
        }
    }

    Ok(())
}

fn execute_select(
    db_file: &mut DBFile,
    schema: &SchemaObject,
    root_page: BTreePage,
    select_statement: &SelectStatement,
) -> Result<()> {
    match root_page.page_type {
        PageType::LeafTable => {
            let column_map = schema.column_map().context("retrieving column order")?;
            let column_indices: Vec<usize> = select_statement
                .select
                .iter()
                .map(|col| column_map[col])
                .collect();

            let cells = root_page
                .read_cells()
                .context("reading cells from root page")?;

            let (where_column_ind, where_value) = match &select_statement.where_clause {
                Some(where_clause) => (
                    column_map.get(&where_clause.column),
                    Some(where_clause.value.as_str()),
                ),
                None => (None, None),
            };

            for cell in cells {
                if cell.len() == 0 {
                    continue;
                }

                if let Some(&ind) = where_column_ind {
                    if !&cell[ind].to_string().eq(where_value.unwrap()) {
                        continue;
                    }
                }
                println!("{}", column_indices.iter().map(|&i| &cell[i]).join("|"));
            }
        }
        PageType::InteriorTable => {
            let cells = root_page
                .read_interior_cells()
                .context("reading interior cells")?;

            for cell in cells {
                let page = db_file
                    .load_page_at(cell.left_child_page as usize)
                    .context("loading page")?;
                execute_select(db_file, schema, page, select_statement).context("dumping page")?;
            }
        }
        _ => bail!("unhandled page type"),
    }

    Ok(())
}
