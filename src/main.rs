use anyhow::{bail, Error, Result};
use sqlite_starter_rust::{
    header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
};
use std::collections::HashMap;
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

            Ok(())
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
            Ok(())
        }

        v => {
            let db_header = read_db_header(&database)?;
            if v.to_lowercase().contains("count(*)") {
                count_rows_in_table(v, db_header, &database)
            } else {
                read_columns(v, db_header, &database)
            }
        }
    }
}

fn read_columns(query: &str, db_header: DBHeader, database: &[u8]) -> Result<(), Error> {
    let (columns, table) = read_column_and_table(query);
    // Assume it's valid SQL

    let schema = db_header
        .schemas
        .into_iter()
        .find(|schema| schema.table_name == table)
        .unwrap();

    let column_map = find_column_positions(&schema.sql);

    let table_page_offset = db_header.page_size as usize * (schema.root_page as usize - 1);
    let page_header =
        PageHeader::parse(&database[table_page_offset..table_page_offset + 8]).unwrap();

    let cell_pointers = database[table_page_offset + 8..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

    let rows = cell_pointers.into_iter().map(|cp| {
        let stream = &database[table_page_offset + cp as usize..];
        let (_, offset) = parse_varint(stream);
        let (_, read_bytes) = parse_varint(&stream[offset..]);

        parse_record(&stream[offset + read_bytes..], 3).unwrap()
    });

    for row in rows {
        let cpos = *column_map.get(&columns[0]).unwrap();

        println!("{}", String::from_utf8_lossy(&row[cpos]));
    }

    Ok(())
}

struct DBHeader {
    page_size: u16,
    schemas: Vec<Schema>,
}

fn read_db_header(database: &[u8]) -> Result<DBHeader, Error> {
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
    let schemas = cell_pointers.into_iter().map(|cell_pointer| {
        let stream = &database[cell_pointer as usize..];
        let (_, offset) = parse_varint(stream);
        let (rowid, read_bytes) = parse_varint(&stream[offset..]);

        parse_record(&stream[offset + read_bytes..], 5)
            .map(|record| Schema::parse(record).expect("Invalid record"))
            .unwrap()
    });

    Ok(DBHeader {
        page_size: db_page_size,
        schemas: schemas.collect(),
    })
}

fn count_rows_in_table(query: &str, db_header: DBHeader, database: &[u8]) -> Result<(), Error> {
    let (_, table) = read_column_and_table(query);
    // Assume it's valid SQL

    let schema = db_header
        .schemas
        .into_iter()
        .find(|schema| schema.table_name == table)
        .unwrap();

    let table_page_offset = db_header.page_size as usize * (schema.root_page as usize - 1);
    let page_header =
        PageHeader::parse(&database[table_page_offset..table_page_offset + 8]).unwrap();

    println!("{}", page_header.number_of_cells);

    Ok(())
}
fn find_column_positions(schema: &str) -> HashMap<&str, usize> {
    let schema = schema.trim_start_matches(|c| c != '(');
    let schema = schema
        .split(',')
        .map(|x| x.trim_matches(|c| c == ' ' || c == '\n' || c == '('))
        .map(|c| c.split(' ').next().unwrap())
        .map(|c| c.trim());

    schema
        .into_iter()
        .enumerate()
        .map(|(i, x)| (x, i))
        .collect()
}

fn read_column_and_table(query: &str) -> (Vec<&str>, &str) {
    let mut query = query.split(' ');
    query.next();
    let column = query.next().unwrap();
    let column = column
        .split(',')
        .filter(|c| !c.is_empty())
        .map(|c| c.trim())
        .collect();
    query.next();
    let table = query.next().unwrap();

    (column, table)
}
