use anyhow::{bail, Error, Result};
use once_cell::sync::Lazy;
use regex::{Regex, RegexBuilder};
use sqlite_starter_rust::header::BTreePage;
use sqlite_starter_rust::record::{ColumnValue, RecordMeta};
use sqlite_starter_rust::{
    header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;

const QUERY_REGEX: Lazy<Regex> = Lazy::new(|| {
    let regex = "select ([a-zA-Z0-9*].*) FROM ([a-zA-Z0-9].*)";

    RegexBuilder::new(regex)
        .case_insensitive(true)
        .build()
        .expect("error in compiling regex")
});

const WHERE_REGEX: Lazy<Regex> = Lazy::new(|| {
    let regex =
        "select ([a-zA-Z0-9*].*) FROM ([a-zA-Z0-9].*) WHERE ([a-zA-Z0-9].*) = ([a-zA-Z0-9'].*)";

    RegexBuilder::new(regex)
        .case_insensitive(true)
        .build()
        .expect("error in compiling regex")
});

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
    match command.as_str().trim() {
        ".dbinfo" => {
            // Parse page header from database
            let (_, page_header) = PageHeader::parse(&database[100..108])?;

            // Obtain all cell pointers
            let cell_pointers = database[108..]
                .chunks_exact(2)
                .take(page_header.number_of_cells.into())
                .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

            // Obtain all records from column 5
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
            let (_, page_header) = PageHeader::parse(&database[100..108])?;

            // Obtain all cell pointers
            let cell_pointers = database[108..]
                .chunks_exact(2)
                .take(page_header.number_of_cells.into())
                .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

            // Obtain all records from column 5
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

        v if v.contains("companies") => {
            let db_header = read_db_header(&database)?;

            // Traverse the index
            read_index(&database, v, &db_header);

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

fn read_index(database: &[u8], query: &str, db_header: &DBHeader) {
    let (columns, table, where_clause) = read_column_and_table(query);

    let schema = db_header
        .schemas
        .iter()
        .find(|schema| schema.table_name == table)
        .unwrap();

    let column_map = find_column_positions(&schema.sql);

    // Assume it's valid SQL
    let index_schema = db_header
        .schemas
        .iter()
        .find(|schema| schema.name == "idx_companies_country")
        .unwrap();

    let rows = parse_page(
        database,
        &db_header,
        &column_map,
        db_header.page_size as usize * (index_schema.root_page as usize - 1),
    );

    let rowids: HashSet<usize> = rows
        .unwrap()
        .filter_map(|(rowid, row)| {
            let record = parse_record(&database[row.offset..], 2);
            let record = record.unwrap();

            if record[0].to_string() == where_clause.unwrap().1 {
                Some(rowid)
            } else {
                None
            }
        })
        .map(|rowid| rowid.read_usize())
        .collect();

    let rows = parse_page(
        database,
        &db_header,
        &column_map,
        db_header.page_size as usize * (schema.root_page as usize - 1),
    )
    .unwrap()
    .filter(|(rowid, _)| rowids.contains(&rowid.read_usize()));

    for (rowid, row) in rows {
        let mut output = String::new();

        for &column in columns.iter() {
            if column == "id" {
                output.push_str(&rowid.to_string());
            } else {
                let cpos = *column_map.get(column).unwrap();
                let record = parse_record(&database[row.offset..], row.column_count);
                let record = record.unwrap();

                output.push_str(&record[cpos].to_string());
            }
            output.push('|');
        }

        let output = output.trim_end_matches(|c| c == '|');

        println!("{}", output);
    }
}

fn parse_page<'a>(
    database: &'a [u8],
    db_header: &'a DBHeader,
    column_map: &'a HashMap<&str, usize>,
    table_page_offset: usize,
) -> Option<Box<dyn Iterator<Item = (ColumnValue<'a>, RecordMeta)> + 'a>> {
    let (read, page_header) =
        PageHeader::parse(&database[table_page_offset..table_page_offset + 12]).unwrap();

    let cell_pointers = database[table_page_offset + read..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()));

    match page_header.page_type {
        BTreePage::InteriorTable => {
            let rows = cell_pointers
                .into_iter()
                .filter_map(move |cp| {
                    let stream = &database[table_page_offset + cp as usize..];
                    let left_child_id =
                        u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]);

                    let (_rowid, _offset) = parse_varint(&stream[4..]);

                    parse_page(
                        database,
                        db_header,
                        column_map,
                        db_header.page_size as usize * (left_child_id as usize - 1),
                    )
                })
                .flatten();

            if let Some(rp) = page_header.right_most_pointer {
                Some(Box::new(
                    rows.chain(
                        parse_page(
                            database,
                            db_header,
                            column_map,
                            db_header.page_size as usize * (rp as usize - 1),
                        )
                        .unwrap(),
                    ),
                ))
            } else {
                Some(Box::new(rows))
            }
        }
        BTreePage::LeafTable => {
            let rows = cell_pointers.into_iter().map(move |cp| {
                let stream = &database[table_page_offset + cp as usize..];
                let (_total, offset) = parse_varint(stream);
                let (rowid, read_bytes) = parse_varint(&stream[offset..]);

                (
                    ColumnValue::U64(rowid as u64),
                    RecordMeta {
                        column_count: column_map.len(),
                        offset: offset + read_bytes + table_page_offset + cp as usize,
                    },
                )
            });

            Some(Box::new(rows))
        }
        BTreePage::InteriorIndex => {
            let rows = cell_pointers
                .into_iter()
                .filter_map(move |cp| {
                    let stream = &database[table_page_offset + cp as usize..];
                    let left_child_id =
                        u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]);
                    let (payload_size, offset) = parse_varint(&stream[4..]);
                    /*
                     *
                     * There is some payload here but it only contains the key so we are just going
                     * to ignore it
                     */
                    let record = parse_record(&stream[offset + 4..offset + 4 + payload_size], 2);
                    let record = record.unwrap();

                    Some(
                        parse_page(
                            database,
                            db_header,
                            column_map,
                            db_header.page_size as usize * (left_child_id as usize - 1),
                        )
                        .unwrap()
                        .chain(std::iter::once((
                            record[1],
                            RecordMeta {
                                column_count: 2,
                                offset: offset + 4 + table_page_offset + cp as usize,
                            },
                        ))),
                    )

                    //                    println!(
                    //                        "left child id = {} payload size = {} offset = {} column count = {} country = {}",
                    //                        left_child_id,
                    //                        payload_size,
                    //                        offset,
                    //                        column_map.len(),country
                    //                    );
                    //
                    // TODO(ishan): Read number of bytes of payload.
                    // Read any over flow pages properly
                    //parse_record(
                    //    &stream[offset + 4..offset + payload_size + 4],
                    //    column_map.len(),
                    //)
                    //.unwrap(),
                })
                .flatten();

            if let Some(rp) = page_header.right_most_pointer {
                Some(Box::new(
                    rows.chain(
                        parse_page(
                            database,
                            db_header,
                            column_map,
                            db_header.page_size as usize * (rp as usize - 1),
                        )
                        .unwrap(),
                    ),
                ))
            } else {
                Some(Box::new(rows))
            }
        }

        BTreePage::LeafIndex => {
            let rows = cell_pointers.into_iter().filter_map(move |cp| {
                let stream = &database[table_page_offset + cp as usize..];
                let (payload_size, offset) = parse_varint(&stream);
                let record = parse_record(&stream[offset..offset + payload_size], 2);
                let record = record.unwrap();

                Some((
                    record[1],
                    RecordMeta {
                        column_count: 2,
                        offset: offset + table_page_offset + cp as usize,
                    },
                ))
            });

            Some(Box::new(rows))
        }
    }
}
fn read_columns(query: &str, db_header: DBHeader, database: &[u8]) -> Result<(), Error> {
    let (columns, table, where_clause) = read_column_and_table(query);
    // Assume it's valid SQL
    let schema = db_header
        .schemas
        .iter()
        .find(|schema| schema.table_name == table)
        .unwrap();

    let column_map = find_column_positions(&schema.sql);

    let rows = parse_page(
        database,
        &db_header,
        &column_map,
        db_header.page_size as usize * (schema.root_page as usize - 1),
    );

    for (rowid, row) in rows.unwrap() {
        let mut output = String::new();

        if let Some(wc) = where_clause {
            let colidx = *column_map.get(wc.0).unwrap();
            let record = parse_record(&database[row.offset..], row.column_count);
            let record = record.unwrap();

            let row_pol = record[colidx].to_string();

            if row_pol != wc.1 {
                continue;
            }
        }

        for &column in columns.iter() {
            if column == "id" {
                output.push_str(&rowid.to_string());
            } else {
                let cpos = *column_map.get(column).unwrap();
                let record = parse_record(&database[row.offset..], row.column_count);
                let record = record.unwrap();

                output.push_str(&record[cpos].to_string());
            }
            output.push('|');
        }

        let output = output.trim_end_matches(|c| c == '|');

        println!("{}", output);
    }

    Ok(())
}

#[derive(Debug)]
struct DBHeader {
    page_size: u16,
    schemas: Vec<Schema>,
}

fn read_db_header(database: &[u8]) -> Result<DBHeader, Error> {
    let db_page_size = u16::from_be_bytes([database[16], database[17]]);
    // Parse page header from database
    let (_, page_header) = PageHeader::parse(&database[100..108])?;

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
    let (_, table, _) = read_column_and_table(query);
    // Assume it's valid SQL

    let schema = db_header
        .schemas
        .into_iter()
        .find(|schema| schema.table_name == table)
        .unwrap();

    let table_page_offset = db_header.page_size as usize * (schema.root_page as usize - 1);
    let (_, page_header) =
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

fn read_column_and_table(query: &str) -> (Vec<&str>, &str, Option<(&str, &str)>) {
    if let Some(matches) = WHERE_REGEX.captures(query) {
        let parameter = matches.get(3).unwrap().as_str().trim();
        let value = matches.get(4).unwrap().as_str().trim();
        let columns = matches.get(1).unwrap().as_str();
        let table = matches.get(2).unwrap().as_str();
        let table: &str = table.trim_matches(|c: char| !c.is_alphabetic());
        let column = columns
            .split(',')
            .filter(|c| !c.is_empty())
            .map(|c| c.trim())
            .collect();

        return (
            column,
            table,
            Some((parameter, value.trim_matches(|c| c == '\''))),
        );
    }

    let matches = QUERY_REGEX.captures(query).unwrap();

    let columns = matches.get(1).unwrap().as_str();
    let table = matches.get(2).unwrap().as_str();
    let table: &str = table.trim_matches(|c: char| !c.is_alphabetic());

    let column = columns
        .split(',')
        .filter(|c| !c.is_empty())
        .map(|c| c.trim())
        .collect();

    (column, table, None)
}
