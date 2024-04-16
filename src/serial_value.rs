use anyhow::{bail, Context, Result};
use std::fmt::{Display, Formatter};
use std::io::Read;

use byteorder::{BigEndian, ReadBytesExt};

#[derive(Debug, Clone)]
/// https://www.sqlite.org/fileformat2.html#record_format
pub enum SerialValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    Zero,
    One,
    Blob(Vec<u8>),
    Text(String),
}

impl SerialValue {
    pub fn read(serial_type: u64, reader: &mut impl Read) -> Result<Self> {
        match serial_type {
            0 => Ok(Self::Null),
            1 => Ok(Self::Int8(reader.read_i8()?)),
            2 => Ok(Self::Int16(reader.read_i16::<BigEndian>()?)),
            3 => Ok(Self::Int24(reader.read_i24::<BigEndian>()?)),
            4 => Ok(Self::Int32(reader.read_i32::<BigEndian>()?)),
            5 => Ok(Self::Int48(reader.read_i48::<BigEndian>()?)),
            6 => Ok(Self::Int64(reader.read_i64::<BigEndian>()?)),
            7 => Ok(Self::Float64(reader.read_f64::<BigEndian>()?)),
            8 => Ok(Self::Zero),
            9 => Ok(Self::One),
            10 | 11 => bail!("unexpected reserved serial value"),
            t if t % 2 == 0 => Ok(Self::Blob({
                let mut bytes = vec![0u8; (t as usize - 12) / 2];
                reader
                    .read_exact(&mut bytes)
                    .context("reading blob bytes")?;
                bytes
            })),
            t if t % 2 == 1 => Ok(Self::Text({
                let mut bytes = vec![0u8; (t as usize - 13) / 2];
                reader
                    .read_exact(&mut bytes)
                    .context("reading text bytes")?;
                String::from_utf8(bytes).context("text bytes into String")?
            })),
            _ => unreachable!(),
        }
    }

    pub fn as_rowid(&self) -> Option<u64> {
        match self {
            Self::Int8(i) => Some(*i as u64),
            Self::Int16(i) => Some(*i as u64),
            Self::Int24(i) | Self::Int32(i) => Some(*i as u64),
            Self::Int48(i) | Self::Int64(i) => Some(*i as u64),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Self::Int8(i) => Some(*i as usize),
            Self::Int16(i) => Some(*i as usize),
            Self::Int24(i) | Self::Int32(i) => Some(*i as usize),
            Self::Int48(i) | Self::Int64(i) => Some(*i as usize),
            _ => None,
        }
    }
}

impl Display for SerialValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "(null)"),
            Self::Zero => write!(f, "0"),
            Self::One => write!(f, "1"),
            Self::Int8(i) => write!(f, "{}", i),
            Self::Int16(i) => write!(f, "{}", i),
            Self::Int24(i) | Self::Int32(i) => write!(f, "{}", i),
            Self::Int48(i) | Self::Int64(i) => write!(f, "{}", i),
            Self::Float64(n) => write!(f, "{}", n),
            Self::Text(t) => write!(f, "{}", t),
            Self::Blob(v) => write!(f, "{:?}", v),
        }
    }
}
