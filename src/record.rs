use crate::varint::parse_varint;
use anyhow::{bail, Result};
use std::fmt::Display;

/// Reads SQLite's "Record Format" as mentioned here:
/// [record_format](https://www.sqlite.org/fileformat.html#record_format)
pub fn parse_record(stream: &[u8], column_count: usize) -> Result<Vec<ColumnValue>> {
    // Parse number of bytes in header, and use bytes_read as offset
    let (header_size, mut offset) = parse_varint(stream);

    // Read each varint into serial types and modify the offset
    let mut serial_types = vec![];
    for _ in 0..column_count {
        let (varint, read_bytes) = parse_varint(&stream[offset..]);
        offset += read_bytes;
        serial_types.push(varint);
    }

    offset = header_size;
    // Parse each serial type as column into record and modify the offset
    let mut record = vec![];
    for serial_type in serial_types {
        let column = parse_column_value(&stream[offset..], serial_type as usize)?;
        offset += column.length();
        record.push(column);
    }

    Ok(record)
}

#[derive(Debug, Copy, Clone)]
pub enum ColumnValue<'a> {
    Null,
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
    U48(u64),
    U64(u64),
    FP64(f64),
    False,
    True,
    Blob(&'a [u8]),
    Text(&'a [u8]),
}

impl<'a> ColumnValue<'a> {
    pub fn length(&self) -> usize {
        match self {
            ColumnValue::Null => 0,
            ColumnValue::U8(_) => 1,
            ColumnValue::U16(_) => 2,
            ColumnValue::U24(_) => 3,
            ColumnValue::U32(_) => 4,
            ColumnValue::U48(_) => 6,
            ColumnValue::U64(_) => 8,
            ColumnValue::FP64(_) => 8,
            ColumnValue::False => 0,
            ColumnValue::True => 0,
            ColumnValue::Blob(v) => v.len(),
            ColumnValue::Text(v) => v.len(),
        }
    }

    pub fn read_u32(&self) -> u32 {
        match self {
            ColumnValue::U8(v) => *v as u32,
            ColumnValue::U16(v) => *v as u32,
            ColumnValue::U24(v) => *v as u32,
            ColumnValue::U32(v) => *v as u32,
            v => {
                println!("{:?}", v);

                unreachable!()
            }
        }
    }

    pub fn read_usize(&self) -> usize {
        match self {
            ColumnValue::U8(v) => *v as usize,
            ColumnValue::U16(v) => *v as usize,
            ColumnValue::U24(v) => *v as usize,
            ColumnValue::U32(v) => *v as usize,
            ColumnValue::U48(v) => *v as usize,
            ColumnValue::U64(v) => *v as usize,
            v => {
                println!("{:?}", v);

                unreachable!()
            }
        }
    }
}

impl<'a> Display for ColumnValue<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Null => f.write_str(""),
            ColumnValue::U8(v) => f.write_str(&v.to_string()),
            ColumnValue::U16(v) => f.write_str(&v.to_string()),
            ColumnValue::U24(v) => f.write_str(&v.to_string()),
            ColumnValue::U32(v) => f.write_str(&v.to_string()),
            ColumnValue::U48(v) => f.write_str(&v.to_string()),
            ColumnValue::U64(v) => f.write_str(&v.to_string()),
            ColumnValue::FP64(v) => f.write_str(&v.to_string()),
            ColumnValue::False => f.write_str("false"),
            ColumnValue::True => f.write_str("true"),
            ColumnValue::Blob(v) => f.write_fmt(format_args!("{:?}", v)),
            ColumnValue::Text(v) => f.write_str(&String::from_utf8(v.to_vec()).unwrap()),
        }
    }
}

fn parse_column_value(stream: &[u8], serial_type: usize) -> Result<ColumnValue> {
    Ok(match serial_type {
        0 => ColumnValue::Null,
        // 8 bit twos-complement integer
        1 => ColumnValue::U8(stream[0]),
        2 => ColumnValue::U16(u16::from_be_bytes([stream[0], stream[1]])),

        3 => ColumnValue::U24(u32::from_be_bytes([0, stream[0], stream[1], stream[2]])),

        4 => ColumnValue::U32(u32::from_be_bytes([
            stream[0], stream[1], stream[2], stream[3],
        ])),

        8 => ColumnValue::False,
        9 => ColumnValue::True,

        // Text encoding
        n if serial_type >= 12 && serial_type % 2 == 0 => {
            let n_bytes = (n - 12) / 2;
            ColumnValue::Blob(&stream[0..n_bytes as usize])
        }
        n if serial_type >= 13 && serial_type % 2 == 1 => {
            let n_bytes = (n - 13) / 2;
            let a = &stream[0..n_bytes as usize];

            ColumnValue::Text(a)
        }
        _ => bail!("Invalid serial_type: {}", serial_type),
    })
}
