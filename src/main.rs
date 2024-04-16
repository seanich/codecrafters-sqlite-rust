use std::cmp::Ordering;
use std::fs::File;

use anyhow::{bail, Context, Result};
use itertools::Itertools;

use sqlite_starter_rust::btree_page::{BTreePage, InteriorCell, PageType};
use sqlite_starter_rust::db_file::DBFile;
use sqlite_starter_rust::schema_object::{ObjectType, SchemaObject};
use sqlite_starter_rust::serial_value::SerialValue;
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
                if ObjectType::Table == schema_obj.object_type
                    && !schema_obj.table_name.starts_with(SQLITE_TABLE_PREFIX)
                {
                    table_names.push(schema_obj.table_name);
                }
            }
            println!("{}", table_names.join(" "));
        }
        ".tableslong" => {
            let mut file = File::open(&args[1])?;
            let db_file = DBFile::new(&mut file).context("constructing DBFile")?;

            for schema_obj in db_file.first_page.load_schemas().context("load schemas")? {
                if ObjectType::Table == schema_obj.object_type
                    && !schema_obj.table_name.starts_with(SQLITE_TABLE_PREFIX)
                {
                    println!("{}: {}", schema_obj.table_name, schema_obj.sql);
                }
            }
        }
        ".indexes" => {
            let mut file = File::open(&args[1])?;
            let db_file = DBFile::new(&mut file).context("constructing DBFile")?;

            for schema_obj in db_file.first_page.load_schemas().context("load schemas")? {
                if ObjectType::Index == schema_obj.object_type {
                    println!(
                        "{} on {}:\n\t{}",
                        schema_obj.name, schema_obj.table_name, schema_obj.sql
                    )
                }
            }
        }
        command => {
            let statement = sql_statement(command).context("parsing SQL statement")?;
            match statement {
                Statement::Select(s) => {
                    let mut file = File::open(&args[1])?;
                    let mut db_file = DBFile::new(&mut file).context("constructing DBFile")?;

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

                    if s.select.len() == 1 && s.select[0].eq_ignore_ascii_case("count(*)") {
                        // TODO: We don't really need to go and retrieve the rows to get a count
                        // if there's an index.
                        println!("{}", select_rows(&mut db_file, root_page, &s)?.len());
                    } else {
                        return select_and_print(&mut db_file, &schema, root_page, &s);
                    }
                }
                Statement::CreateTable(_) | Statement::CreateIndex(_) => {
                    bail!("create statements not supported")
                }
            }
        }
    }

    Ok(())
}

fn select_and_print(
    db_file: &mut DBFile,
    schema: &SchemaObject,
    root_page: BTreePage,
    select_statement: &SelectStatement,
) -> Result<()> {
    let column_map = schema.column_map().context("retrieving column order")?;
    let column_indices: Vec<usize> = select_statement
        .select
        .iter()
        .map(|col| column_map[col])
        .collect();

    let rows = select_rows(db_file, root_page, select_statement)?;
    match &select_statement.where_clause {
        Some(where_clause) => {
            let where_col_ind = column_map
                .get(where_clause.column.as_str())
                .copied()
                .context("finding index of where column")?;
            let where_val = where_clause.value.as_str();

            for row in rows {
                if &row[where_col_ind].to_string() == where_val {
                    print_row(row, &column_indices)
                }
            }
        }
        None => {
            for row in rows {
                print_row(row, &column_indices)
            }
        }
    };

    Ok(())
}

fn select_rows(
    db_file: &mut DBFile,
    root_page: BTreePage,
    select_statement: &SelectStatement,
) -> Result<Vec<Vec<SerialValue>>> {
    // If there is a where clause, try to load an index for the given filter column. If an
    // index is found, load the matching row_id's from the index.
    let index_row_ids: Option<Vec<u64>> = match &select_statement.where_clause {
        Some(where_clause) => {
            let index_page = db_file
                .get_index_page(&select_statement.from, &where_clause.column)
                .context("finding index page")?;

            match index_page {
                Some(pos) => {
                    let page = db_file.load_page_at(pos)?;
                    Some(search_index(db_file, page, &where_clause.value)?)
                }
                None => None,
            }
        }
        None => None,
    };

    match index_row_ids {
        Some(row_ids) => select_with_index(db_file, root_page, &row_ids),
        None => select_without_index(db_file, root_page),
    }
}

fn select_without_index(db_file: &mut DBFile, page: BTreePage) -> Result<Vec<Vec<SerialValue>>> {
    let mut result = vec![];

    match page.page_type {
        PageType::LeafTable => {
            // TODO: It would be possible to pass the column indices we want to this function and
            // skip over the serial values for any columns we don't care about.
            let cells = page.read_cells().context("reading cells from root page")?;
            for cell in cells {
                result.push(cell)
            }
        }
        PageType::InteriorTable => {
            let cells = page
                .read_interior_cells()
                .context("reading interior cells")?;

            for interior_cell in cells {
                let InteriorCell::Table(cell) = interior_cell else {
                    bail!("invalid cell type")
                };
                let page = db_file
                    .load_page_at(cell.left_child_page as usize)
                    .context("loading page")?;
                result.extend(select_without_index(db_file, page)?);
            }

            if let Some(right_ptr) = page.right_most_pointer {
                let page = db_file
                    .load_page_at(right_ptr as usize)
                    .context("loading page")?;
                result.extend(select_without_index(db_file, page)?);
            }
        }
        _ => bail!("unhandled page type"),
    }

    Ok(result)
}

fn select_with_index(
    db_file: &mut DBFile,
    page: BTreePage,
    row_ids: &[u64],
) -> Result<Vec<Vec<SerialValue>>> {
    let mut results = vec![];
    match page.page_type {
        PageType::InteriorTable => {
            let cells = page
                .read_interior_cells()
                .context("reading interior cells")?;

            let mut right_ids = row_ids;

            for (ind, interior_cell) in cells.iter().enumerate() {
                let InteriorCell::Table(cell) = interior_cell else {
                    bail!("invalid cell type - expected interior table cell")
                };

                let pp = right_ids.partition_point(|&id| id <= cell.row_id);
                let left_ids = &right_ids[..pp];
                right_ids = &right_ids[pp..];

                if !left_ids.is_empty() {
                    // The left page of this BTree item or its child pages should contain the IDs in
                    // left_ids. Load that page then add its select results to the result set.
                    let next_page = db_file
                        .load_page_at(cell.left_child_page as usize)
                        .context("loading next index page")?;

                    results.extend(
                        select_with_index(db_file, next_page, &left_ids)
                            .context("loading results from next index page")?,
                    );
                }

                if right_ids.is_empty() {
                    // No more rows to find in  this page
                    break;
                }

                if let Some(right_page) = page.right_most_pointer {
                    if ind == cells.len() - 1 {
                        // There might be additional results in the right page pointer
                        let right_page = db_file
                            .load_page_at(right_page as usize)
                            .context("loading right page")?;

                        results.extend(
                            select_with_index(db_file, right_page, right_ids)
                                .context("searching in right index page")?,
                        );
                    }
                }
            }
        }
        PageType::LeafTable => {
            let mut cells = page
                .read_cells()
                .context("reading cells from leaf table page")?
                .into_iter();

            for &id in row_ids {
                results.push(
                    cells
                        .by_ref()
                        .skip_while(|c| match c[0].as_rowid() {
                            Some(rowid) => rowid < id,
                            None => unreachable!(),
                        })
                        .next()
                        .context("must have a value")?,
                );
            }
        }
        _ => unreachable!(),
    }
    Ok(results)
}

// Searches an index starting from the given page and returns the rowids for any values matching the
// query.
fn search_index(db_file: &mut DBFile, page: BTreePage, query: &str) -> Result<Vec<u64>> {
    match page.page_type {
        PageType::InteriorIndex => {
            let cells = page
                .read_interior_cells()
                .context("reading interior cells")?;

            let mut results = vec![];
            for (ind, interior_cell) in cells.iter().enumerate() {
                let InteriorCell::Index(cell) = interior_cell else {
                    bail!("invalid cell type")
                };

                // TODO: Handle checking types properly
                let cell_content = &cell.columns[0].to_string();

                let cell_content = cell_content.as_str();
                let cell_cmp = cell_content.cmp(query);

                if cell_cmp == Ordering::Greater || cell_cmp == Ordering::Equal {
                    // The left page of this BTree item _might_ contain more matching entries so
                    // load that page and add any rowids it produces to the result set.
                    let next_page = db_file
                        .load_page_at(cell.left_child_page as usize)
                        .context("loading next index page")?;

                    results.extend(
                        search_index(db_file, next_page, query)
                            .context("loading results from next index page")?,
                    );
                }

                if cell_cmp == Ordering::Greater {
                    // The following BTree items _cannot_ contain the search query - we can bail out
                    // from the loop now
                    break;
                }

                if cell_cmp == Ordering::Equal {
                    // This cell matches the query - add the rowid to the result set.
                    results.push(cell.rowid);
                }

                if let Some(right_page) = page.right_most_pointer {
                    if ind == cells.len() - 1
                        && (cell_cmp == Ordering::Equal || cell_cmp == Ordering::Less)
                    {
                        // There might be additional results in the right page pointer
                        let right_page = db_file
                            .load_page_at(right_page as usize)
                            .context("loading right page")?;

                        results.extend(
                            search_index(db_file, right_page, query)
                                .context("searching in right index page")?,
                        )
                    }
                }
            }
            Ok(results)
        }
        PageType::LeafIndex => {
            // TODO: It might make sense to do a binary search over the cells on leaf pages
            // These cells are laid out as [Serial(<indexed column>)..., Int?(<rowid>)]
            Ok(page
                .read_cells()?
                .into_iter()
                .filter(|c| &c[0].to_string() == query)
                .map(|c| c[1].as_rowid().unwrap_or_else(|| 0u64))
                .collect())
        }
        _ => unreachable!(),
    }
}

fn print_row(row: Vec<SerialValue>, indices: &[usize]) {
    println!("{}", indices.into_iter().map(|ind| &row[*ind]).join("|"))
}
