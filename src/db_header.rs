use anyhow::{bail, Result};

/// https://www.sqlite.org/fileformat.html#the_database_header
#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[repr(packed)]
pub struct DBHeader {
    /// The header string: "SQLite format 3\000"
    header_string: [u8; 16],
    /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or
    /// the value 1 representing a page size of 65536.
    page_size: [u8; 2],
    /// File format write version. 1 for legacy; 2 for WAL.
    write_version: u8,
    /// File format read version. 1 for legacy; 2 for WAL.
    read_version: u8,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    page_reserved_bytes: u8,
    /// Maximum embedded payload fraction. Must be 64.
    max_embedded_payload_fraction: u8,
    /// Minimum embedded payload fraction. Must be 32.
    min_embedded_payload_fraction: u8,
    /// Leaf payload fraction. Must be 32.
    leaf_payload_fraction: u8,
    /// File change counter.
    file_change_counter: [u8; 4],
    /// Size of the database file in pages. The "in-header database size".
    in_header_db_size: [u8; 4],
    /// Page number of the first freelist trunk page.
    first_freelist_trunk_page: [u8; 4],
    /// Total number of freelist pages.
    total_freelist_pages: [u8; 4],
    /// The schema cookie.
    schema_cookie: [u8; 4],
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    schema_format_number: [u8; 4],
    /// Default page cache size.
    default_page_cache_size: [u8; 4],
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    largest_root_b_tree_page: [u8; 4],
    /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    db_text_encoding: [u8; 4],
    /// The "user version" as read and set by the user_version pragma.
    user_version: [u8; 4],
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incremental_vacuum: [u8; 4],
    /// The "Application ID" set by PRAGMA application_id.
    application_id: [u8; 4],
    /// Reserved for expansion. Must be zero.
    _reserved: [u8; 20],
    /// The version-valid-for number.
    version_valid_for: [u8; 4],
    /// SQLITE_VERSION_NUMBER
    version_number: [u8; 4],
}

macro_rules! field_decoder {
    ($type:ty; $name:ident) => {
        pub fn $name(&self) -> $type {
            <$type>::from_be_bytes(self.$name)
        }
    };
}

impl DBHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() != Self::SIZE {
            bail!("Input data is wrong size to construct DB header")
        }

        let header = data as *const [u8] as *const Self;
        Ok(unsafe { *header })
    }

    field_decoder! {u16; page_size}
}
