use polars::prelude::*;
use std::error::Error;
use std::path::PathBuf;
use std::env;
use std::fs::{File, create_dir_all};

fn process_and_write_csv<F>(input_path: &PathBuf, output_path: &PathBuf, transform: F) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(LazyFrame) -> LazyFrame,
{
    // read the CSV file into a LazyFrame
    let lazy_df = LazyCsvReader::new(input_path)
        .with_has_header(true)
        .finish()?;
    // process the data
    let transformed_df = transform(lazy_df);
    let mut df = transformed_df.collect()?;
    // write the DataFrame to the CSV file
    let mut file = File::create(output_path)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .finish(&mut df)?;

    Ok(())
}


fn main() -> Result<(), Box<dyn Error>> {
    // getting projects directories and data folder
    let input_path = env::var("INPUT_PATH").expect("INPUT_PATH is not set");
    let output_path = env::var("OUTPUT_PATH").expect("OUTPUT_PATH is not set");

    // convert to PathBuf for working with paths
    let input_path = PathBuf::from(input_path);
    let output_path = PathBuf::from(output_path).join("polars");
    println!("Input Path: {:?}", input_path);
    println!("Output Path: {:?}", output_path);

    // ensuring output directory exists
    if let Err(e) = create_dir_all(&output_path) {
        eprintln!("Failed to create output directory: {:?}", e);
    } else {
        println!("Output directory is ready at {:?}", output_path);
    }

    // 1) filter by not null
    let filter_null = |lazy_df: LazyFrame| {
        lazy_df.filter(col("int4").is_not_null())
    };
    process_and_write_csv(&input_path, &output_path.join("query_0.csv"), filter_null)?;

    // 2) fill null values with zero 
    let fill_null = |lazy_df: LazyFrame| {
        lazy_df.fill_null(lit(0))
    };
    process_and_write_csv(&input_path, &output_path.join("query_1.csv"), fill_null)?;

    // 3) filter data with two conditions, one of them requiring computation
    let filter_with_comp = |lazy_df: LazyFrame| {
        lazy_df
            .filter(col("int4").is_not_null())
            .filter((col("int4") % lit(2))
            .eq(lit(0)))
    };
    process_and_write_csv(&input_path, &output_path.join("query_2.csv"), filter_with_comp)?;

    // 4) filter out many, group and aggregate with one accumulator
    let filter_aggregate = |lazy_df: LazyFrame| {
        lazy_df
            .filter(col("string1").str().contains_literal(lit("a")))
            .group_by([col("string1")])
            .agg([col("int4").sum().alias("sum_int4")])
            .select([col("string1"), col("sum_int4")])
    };
    process_and_write_csv(&input_path, &output_path.join("query_3.csv"), filter_aggregate)?;

    // 5) group and aggregate with one accumulator
    let group_aggregate = |lazy_df: LazyFrame| {
        lazy_df 
            .group_by([col("string1")]) 
            .agg([col("int4").mean().alias("avg_int4")]) 
            .select([col("string1"), col("avg_int4")])

    };
    process_and_write_csv(&input_path, &output_path.join("query_4.csv"), group_aggregate)?;


    println!("Processing complete. Results saved to {}", output_path.to_str().unwrap());

    Ok(())
}
