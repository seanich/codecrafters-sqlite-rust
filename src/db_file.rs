use crate::btree_page::BTreePage;
use crate::db_header::DBHeader;
use crate::schema_object::{ObjectType, SchemaObject};
use crate::sql::sql::sql_statement;
use crate::sql::Statement;
use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct DBFile<'a> {
    file: &'a mut File,

    pub header: DBHeader,
    pub first_page: BTreePage,
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

        Ok(Self {
            file,
            header: db_header,
            first_page: page,
        })
    }

    pub fn schema_for_table(&mut self, table_name: &str) -> Result<SchemaObject> {
        self.first_page
            .load_schemas()
            .context("loading schemas")?
            .into_iter()
            .find(|s| s.table_name == table_name)
            .ok_or(anyhow!("failed to find table"))
    }

    pub fn get_index_page(&mut self, table_name: &str, column_name: &str) -> Result<Option<usize>> {
        let schema_obj = self
            .first_page
            .load_schemas()
            .context("loading schemas")?
            .into_iter()
            .filter(|s| s.object_type == ObjectType::Index && s.table_name == table_name)
            .find(|s| {
                let statement = sql_statement(&s.sql).expect("parsing index SQL statement");
                if let Statement::CreateIndex(create_index) = statement {
                    return create_index
                        .columns
                        .into_iter()
                        .any(|c| c.as_str() == column_name);
                } else {
                    false
                }
            });

        Ok(match schema_obj {
            None => None,
            Some(o) => o.root_page,
        })
    }

    fn seek_to_page(&mut self, page: usize) -> Result<u64> {
        let page_offset = page - 1; // pages are 1-indexed
        self.file
            .seek(SeekFrom::Start(
                page_offset as u64 * self.header.page_size() as u64,
            ))
            .context("seeking to root page offset")
    }

    pub fn load_page_at(&mut self, page: usize) -> Result<BTreePage> {
        // Seek to page start
        self.seek_to_page(page)?;

        // Load page
        let mut buf = vec![0u8; self.header.page_size() as usize];
        self.file
            .read_exact(&mut buf)
            .context("reading page into buffer")?;

        BTreePage::new(&buf, None)
    }

    pub fn row_count(&mut self, table_name: &str) -> Result<usize> {
        // Find the table schema
        let table_schema = match self.schema_for_table(table_name) {
            Ok(s) => s,
            Err(_) => bail!("could not find table with name '{}'", table_name),
        };

        let page = self
            .load_page_at(table_schema.root_page.context("getting root page offset")?)
            .context("loading BTree page")?;

        // Get number of cells (i.e. row count)
        Ok(page.num_cells as usize)
    }
}
