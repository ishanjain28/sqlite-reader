use anyhow::{bail, Result};
use sqlite_starter_rust::{
    header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
};
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => (),
    }

    // Read database file into database
    let mut file = File::open(&args[1])?;
    let mut database = Vec::new();
    file.read_to_end(&mut database)?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            // Parse page header from database
            let page_header = PageHeader::parse(&database[100..108])?;

            // Obtain all cell pointers
            let cell_pointers = database[108..]
                .chunks_exact(2)
                .take(page_header.number_of_cells.into())
                .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

            // Obtain all records from column 5
            #[allow(unused_variables)]
            let schemas = cell_pointers
                .into_iter()
                .map(|cell_pointer| {
                    let stream = &database[cell_pointer as usize..];
                    let (_, offset) = parse_varint(stream);
                    let (_rowid, read_bytes) = parse_varint(&stream[offset..]);
                    parse_record(&stream[offset + read_bytes..], 5)
                        .map(|record| Schema::parse(record).expect("Invalid record"))
                })
                .collect::<Result<Vec<_>>>()?;

            // You can use print statements as follows for debugging, they'll be visible when running tests.

            print!("number of tables: {}", schemas.len());

            // Uncomment this block to pass the first stage
            // println!("number of tables: {}", schemas.len());
        }

        ".tables" => {
            // Parse page header from database
            let page_header = PageHeader::parse(&database[100..108])?;

            // Obtain all cell pointers
            let cell_pointers = database[108..]
                .chunks_exact(2)
                .take(page_header.number_of_cells.into())
                .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

            // Obtain all records from column 5
            #[allow(unused_variables)]
            let schemas = cell_pointers
                .into_iter()
                .map(|cell_pointer| {
                    let stream = &database[cell_pointer as usize..];
                    let (_, offset) = parse_varint(stream);
                    let (_rowid, read_bytes) = parse_varint(&stream[offset..]);
                    parse_record(&stream[offset + read_bytes..], 5)
                        .map(|record| Schema::parse(record).expect("Invalid record"))
                })
                .collect::<Result<Vec<_>>>()?;

            for schema in schemas
                .into_iter()
                .filter(|schema| !schema.table_name.starts_with("sqlite"))
                .filter(|schema| schema.kind == "table")
            {
                print!("{} ", schema.name);
            }
        }

        v => {
            // Assume it's valid SQL
            let table_name = v.split_whitespace().last().unwrap();

            let db_page_size = u16::from_be_bytes([database[16], database[17]]);

            // Parse page header from database
            let page_header = PageHeader::parse(&database[100..108])?;

            // Obtain all cell pointers
            let cell_pointers = database[108..]
                .chunks_exact(2)
                .take(page_header.number_of_cells.into())
                .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

            // Obtain all records from column 5
            #[allow(unused_variables)]
            let schemas = cell_pointers
                .into_iter()
                .map(|cell_pointer| {
                    let stream = &database[cell_pointer as usize..];
                    let (_, offset) = parse_varint(stream);
                    let (rowid, read_bytes) = parse_varint(&stream[offset..]);

                    parse_record(&stream[offset + read_bytes..], 5)
                        .map(|record| Schema::parse(record).expect("Invalid record"))
                })
                .collect::<Result<Vec<_>>>()?;

            let schema = schemas
                .into_iter()
                .find(|schema| schema.table_name == table_name)
                .unwrap();

            let table_page_offset = db_page_size as usize * (schema.root_page as usize - 1);
            let page_header =
                PageHeader::parse(&database[table_page_offset..table_page_offset + 8]).unwrap();

            println!("{}", page_header.number_of_cells);
        }
    }
    Ok(())
}
