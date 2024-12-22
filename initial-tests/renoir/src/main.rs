use renoir::prelude::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Deserialize, Serialize)]
struct CsvRow {
    int1: u64,
    string1: String,
    int4: Option<u64>
}

fn main() {

    // getting projects directories and data folder
    let input_path = env::var("INPUT_PATH").expect("INPUT_PATH is not set");
    let output_path = env::var("OUTPUT_PATH").expect("OUTPUT_PATH is not set");

    // convert to PathBuf for working with paths
    let input_path = PathBuf::from(input_path);
    let output_path = PathBuf::from(output_path).join("renoir");
    println!("Input Path: {:?}", input_path);
    println!("Output Path: {:?}", output_path);

    // ensuring output directory exists
    if let Err(e) = fs::create_dir_all(&output_path) {
        eprintln!("Failed to create output directory: {:?}", e);
    } else {
        println!("Output directory is ready at {:?}", output_path);
    }

    println!("Starting execution of jobs");

    // renoir initialization
    let config = RuntimeConfig::local(1).unwrap(); // just to have one output csv file
    let ctx = StreamContext::new(config);
    let source = CsvSource::<CsvRow>::new(input_path.join("ints_string.csv"));
    let mut splits = ctx.stream(source).split(5);

    // 1) filter by not null
    let _not_null_values = splits.pop()
        .unwrap()
        .filter(|row| row.int4.is_some())
        .write_csv_seq(output_path.join("query_0.csv"), false);

    // 2) fill null values with zero 
    let _filled_values = splits.pop()
        .unwrap()
        .map(|row| {
            row.int4.unwrap_or(0);
            row
        })
        .write_csv_seq(output_path.join("query_1.csv"), false);

    // 3) filter data with two conditions, one of them requiring computation
    let _filter_even  = splits.pop()
        .unwrap()
        .filter(|row: &CsvRow| (row.int4.is_some() && row.int1 %2 == 0))
        .write_csv_seq(output_path.join("query_2.csv"), false);
    
    // 4) filter out many, group and aggregate with one accumulator
    let _filter_group_by_sum  = splits.pop()
        .unwrap()
        .filter(|row| row.string1.contains("a"))
        .group_by_sum(|row| row.string1.clone(), |row| row.int4.unwrap_or_default())
        .unkey()
        .write_csv_seq(output_path.join("query_3.csv"), false);

    // 5) group and aggregate with one accumulator
    let _group_by_avg  = splits.pop()
        .unwrap()
        .filter(|row| row.string1.contains("a"))
        .group_by_avg(|row| row.string1.clone(), |row| row.int4.unwrap_or_default() as f64)
        .unkey()
        .write_csv_seq(output_path.join("query_4.csv"), false);
 
    
    ctx.execute_blocking();
    
}
