use std::io::Read;

use anyhow::Result;
use byteorder::ReadBytesExt;

pub mod btree_page;
pub mod db_file;
mod db_header;
mod macros;
pub mod schema_object;
mod serial_value;
pub mod sql;

pub trait ReadVarint<T> {
    fn read_varint(&mut self) -> Result<u64>;
}

impl<T> ReadVarint<T> for T
where
    T: Read,
{
    /// A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement
    /// integers that uses less space for small positive values. A varint is between 1 and 9 bytes
    /// in length. The varint consists of either zero or more bytes which have the high-order bit
    /// set followed by a single byte with the high-order bit clear, or nine bytes, whichever is
    /// shorter. The lower seven bits of each of the first eight bytes and all 8 bits of the ninth
    /// byte are used to reconstruct the 64-bit twos-complement integer. Varints are big-endian:
    /// bits taken from the earlier byte of the varint are more significant than bits taken from
    /// the later bytes.
    fn read_varint(&mut self) -> Result<u64> {
        let mut result = 0u64;
        for _ in 0..=8 {
            let a = self.read_u8()?;
            result <<= 7; // make room for this byte
            result += (a & 0b0111_1111) as u64;

            if a & 0b1000_0000 == 0 {
                // first bit is not set - this must be the last byte
                return Ok(result);
            }
        }
        // we got to the 9th byte - use all 8 bits of it
        let a = self.read_u8()?;
        result <<= 8;
        result += a as u64;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn single_byte() {
        let mut data = Cursor::new(vec![120]);
        assert_eq!(data.read_varint().unwrap(), 120)
    }

    #[test]
    fn two_bytes() {
        let mut data = Cursor::new(vec![240, 62]);
        assert_eq!(data.read_varint().unwrap(), 14398)
    }

    #[test]
    fn three_bytes() {
        let mut data = Cursor::new(vec![129, 129, 54]);
        assert_eq!(data.read_varint().unwrap(), 16566)
    }

    // TODO: More tests
}
