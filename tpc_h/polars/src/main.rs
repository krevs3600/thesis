use std::{error::Error, vec};
use std::env;
use polars::prelude::*;
use chrono::NaiveDate;
use reqwest::blocking::Client;
use serde_json::json;

const BENCHMARK: &str = "TPC-H";
const BACKEND: &str = "polars";
const TEST: &str = "tcph_1_gb";

fn send_execution_time(query_id: u32, run_id: u32, execution_time: f64) -> Result<(), Box<dyn Error>> {
    let url = "http://127.0.0.1:5000/execution_time";

    // Construct the JSON payload
    let payload = json!({
        "benchmark": BENCHMARK,
        "backend": BACKEND,
        "test": TEST,
        "query_id": query_id,
        "run_id": run_id,
        "execution_time": execution_time
    });

    // Create a client and send the POST request
    let client = Client::new();
    let response = client
        .post(url)
        .json(&payload)
        .send()?;

    // Check if the response is successful
    if response.status().is_success() {
        println!("Execution time sent successfully");
    } else {
        eprintln!(
            "Failed to send execution time: {}",
            response.text().unwrap_or_else(|_| "Unknown error".to_string())
        );
    }

    Ok(())
}

// to execute all queries
fn execute_all_query(queries: Vec<Arc<dyn Fn() -> LazyFrame + Send + Sync>>){

    let mut query_number = 1;
    for query in queries {
        let start = std::time::Instant::now();
        let result = query().collect();
        let duration = start.elapsed();
        match result {
            Ok(df) => println!("Query {} successful! Result:\n{:?}\nExecution time: {:?}", query_number, df, duration),
            Err(e) => println!("Query failed: {:?}", e),
        }
        if let Err(e) = send_execution_time(query_number, 1, duration.as_secs_f64()) {
            eprintln!("Error: {}", e);
        }
        query_number+=1;
    }
}

// for testing purposes
fn _execute_query_at_index(queries: Vec<Arc<dyn Fn() -> LazyFrame + Send + Sync>>, index : usize){
    let start = std::time::Instant::now();
    let result = queries.get(index).unwrap()().collect();
    let duration = start.elapsed();
    match result {
        Ok(df) => println!("Query successful! Result:\n{:?}\nExecution time: {:?}", df, duration),
        Err(e) => println!("Query failed: {:?}", e),
    }
}




fn main() -> eyre::Result<()> {
    let input_path = env::var("INPUT_PATH").expect("INPUT_PATH is not set");
    let input_path = format!("{}/", input_path);

    let _output_path = "./output/";

    /* ###### read the CSV files into a LazyFrame ###### */ 
    
    // ---- PART ----
    let part = LazyCsvReader::new(format!("{}part.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("P_PARTKEY"), 
            col("column_2").alias("P_NAME"), 
            col("column_3").alias("P_MFGR"), 
            col("column_4").alias("P_BRAND"), 
            col("column_5").alias("P_TYPE"), 
            col("column_6").alias("P_SIZE"), 
            col("column_7").alias("P_CONTAINER"), 
            col("column_8").alias("P_RETAILPRICE"), 
            col("column_9").alias("P_COMMENT")]);
    // ---- SUPPLIER ----
    let supplier = LazyCsvReader::new(format!("{}supplier.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("S_SUPPKEY"), 
            col("column_2").alias("S_NAME"), 
            col("column_3").alias("S_ADDRESS"), 
            col("column_4").alias("S_NATIONKEY"), 
            col("column_5").alias("S_PHONE"), 
            col("column_6").alias("S_ACCTBAL"), 
            col("column_7").alias("S_COMMENT")]);
        
    // ---- PARTSUPP ----
    let partsupp = LazyCsvReader::new(format!("{}partsupp.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("PS_PARTKEY"), 
            col("column_2").alias("PS_SUPPKEY"), 
            col("column_3").alias("PS_AVAILQTY"), 
            col("column_4").alias("PS_SUPPLYCOST"), 
            col("column_5").alias("PS_COMMENT")]);
        
    // ---- CUSTOMER ----
    let customer = LazyCsvReader::new(format!("{}customer.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("C_CUSTKEY"), 
            col("column_2").alias("C_NAME"), 
            col("column_3").alias("C_ADDRESS"), 
            col("column_4").alias("C_NATIONKEY"), 
            col("column_5").alias("C_PHONE"), 
            col("column_6").alias("C_ACCTBAL"), 
            col("column_7").alias("C_MKTSEGMENT"), 
            col("column_8").alias("C_COMMENT")]);
        
    // ---- ORDERS ----
    let orders = LazyCsvReader::new(format!("{}orders.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("O_ORDERKEY"), 
            col("column_2").alias("O_CUSTKEY"), 
            col("column_3").alias("O_ORDERSTATUS"), 
            col("column_4").alias("O_TOTALPRICE"), 
            col("column_5").alias("O_ORDERDATE"), 
            col("column_6").alias("O_ORDERPRIORITY"), 
            col("column_7").alias("O_CLERK"), 
            col("column_8").alias("O_SHIPPRIORITY"), 
            col("column_9").alias("O_COMMENT")])
        .with_column(
            col("O_ORDERDATE").str().to_date(  // Format for the date string
                StrptimeOptions::default()
            ).alias("O_ORDERDATE"));
        
    // ---- LINEITEM ----
    let lineitem = LazyCsvReader::new(format!("{}lineitem.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("L_ORDERKEY"), 
            col("column_2").alias("L_PARTKEY"), 
            col("column_3").alias("L_SUPPKEY"), 
            col("column_4").alias("L_LINENUMBER"), 
            col("column_5").alias("L_QUANTITY"), 
            col("column_6").alias("L_EXTENDEDPRICE"), 
            col("column_7").alias("L_DISCOUNT"), 
            col("column_8").alias("L_TAX"), 
            col("column_9").alias("L_RETURNFLAG"), 
            col("column_10").alias("L_LINESTATUS"), 
            col("column_11").alias("L_SHIPDATE"), 
            col("column_12").alias("L_COMMITDATE"), 
            col("column_13").alias("L_RECEIPTDATE"), 
            col("column_14").alias("L_SHIPINSTRUCT"), 
            col("column_15").alias("L_SHIPMODE"), 
            col("column_16").alias("L_COMMENT")])
        .with_column(
            col("L_SHIPDATE").str().to_date(  // Format for the date string
                StrptimeOptions::default()
            ).alias("L_SHIPDATE"))
        .with_column(
            col("L_RECEIPTDATE").str().to_date(  // Format for the date string
                StrptimeOptions::default()
            ).alias("L_RECEIPTDATE"))
        .with_column(
            col("L_COMMITDATE").str().to_date(  // Format for the date string
                StrptimeOptions::default()
            ).alias("L_COMMITDATE"));
        
    // ---- NATION ----
    let nation = LazyCsvReader::new(format!("{}nation.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("N_NATIONKEY"), 
            col("column_2").alias("N_NAME"), 
            col("column_3").alias("N_REGIONKEY"), 
            col("column_4").alias("N_COMMENT")]);
        

    // ---- REGION ----
    let region = LazyCsvReader::new(format!("{}region.csv", input_path))
        .with_has_header(false)
        .finish()?
        .select(vec![
            col("column_1").alias("R_REGIONKEY"), 
            col("column_2").alias("R_NAME"), 
            col("column_3").alias("R_COMMENT")]);
        


    // Create a vector of closures for each query
    let queries: Vec<Arc<dyn Fn() -> LazyFrame + Send + Sync>> = vec![
        // Query 1
        Arc::new({
            let lineitem1 = lineitem.clone(); 
            move || {
            lineitem1.clone()
                .filter(col("L_SHIPDATE").lt_eq(lit(NaiveDate::from_ymd_opt(1998, 12, 1).unwrap()
                .checked_sub_signed(chrono::Duration::days(90))
                .expect("Valid date"))))
            .group_by(&[col("L_RETURNFLAG"), col("L_LINESTATUS")])
            .agg(&[
                col("L_QUANTITY").sum().alias("SUM_QTY"),
                col("L_EXTENDEDPRICE").sum().alias("SUM_BASE_PRICE"),
                (col("L_EXTENDEDPRICE") * (lit(1) - col("L_DISCOUNT"))).sum().alias("SUM_DISC_PRICE"),
                (col("L_EXTENDEDPRICE") * (lit(1) - col("L_DISCOUNT")) * (lit(1) + col("L_TAX"))).sum().alias("SUM_CHARGE"),
                col("L_QUANTITY").mean().alias("AVG_QTY"),
                col("L_EXTENDEDPRICE").mean().alias("AVG_PRICE"),
                col("L_DISCOUNT").mean().alias("AVG_DISC"),
                col("L_QUANTITY").count().alias("COUNT_ORDER"),
            ])
            .sort(
                ["L_RETURNFLAG", "L_LINESTATUS"],
                SortMultipleOptions::new()
                    .with_order_descending_multi([false, false])
            )}
        }),        
        // Query 2 and other queries as similar closures...
        Arc::new({ 
            let supplier2 = supplier.clone();
            let partsupp2 = partsupp.clone();
            let nation2 = nation.clone();
            let region2 = region.clone();
            let part2 = part.clone();

            move || {

            // Proceed with the final join if previous steps are successful
            let min_part_supp = partsupp2.clone()
                .join(part2.clone(), [col("PS_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                .join(supplier2.clone(), [col("PS_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                .join(nation2.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                .join(region2.clone(), [col("N_REGIONKEY")], [col("R_REGIONKEY")], JoinArgs::default())
                .filter(col("R_NAME").eq(lit("EUROPE")))
                .group_by(&[col("PS_PARTKEY")])
                .agg(&[col("PS_SUPPLYCOST").min()])
                .select(vec![col("PS_PARTKEY"), col("PS_SUPPLYCOST").alias("MIN_SUPPLYCOST")]);
            
               

            part2.clone()
                .join(partsupp2.clone(), [col("P_PARTKEY")], [col("PS_PARTKEY")], JoinArgs::default())
                .join(supplier2.clone(), [col("PS_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                .join(nation2.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                .join(region2.clone(), [col("N_REGIONKEY")], [col("R_REGIONKEY")], JoinArgs::default())
                .join(min_part_supp.clone(), [col("PS_SUPPLYCOST"), col("P_PARTKEY")], [col("MIN_SUPPLYCOST"), col("PS_PARTKEY")], JoinArgs::default())
                .filter(
                    col("P_SIZE").eq(lit(15))
                        .and(col("P_TYPE").str().contains(lit("BRASS"), false))
                        .and(col("R_NAME").eq(lit("EUROPE")))                       
                )
                .select(vec![
                    col("S_ACCTBAL"),
                    col("S_NAME"),
                    col("N_NAME"),
                    col("P_PARTKEY"),
                    col("P_MFGR"),
                    col("S_ADDRESS"),
                    col("S_PHONE"),
                    col("S_COMMENT")//,
                    //col("PS_SUPPLYCOST")
                ])
                .sort(
                    ["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
                    SortMultipleOptions::new()
                        .with_order_descending_multi([true, false, false, false])
                )}
        }),

        //  query 3
        Arc::new({
                let customer3 = customer.clone();
                let orders3 = orders.clone();
                let lineitem3 = lineitem.clone();
             move || {
            customer3.clone()
                .join(orders3.clone(), [col("C_CUSTKEY")], [col("O_CUSTKEY")], JoinArgs::default())
                .join(lineitem3.clone(), [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())
                .group_by(&[col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY")])
                .agg(&[
                    (col("L_EXTENDEDPRICE") * (lit(1) - col("L_DISCOUNT"))).sum().alias("REVENUE")
                ])
                .select(vec![
                    col("O_ORDERKEY"),
                    col("REVENUE"),
                    col("O_ORDERDATE"),
                    col("O_SHIPPRIORITY")
                ])
                .sort(
                    ["REVENUE", "O_ORDERDATE"],
                    SortMultipleOptions::new()
                        .with_order_descending_multi([true, false])
                )}
        }),
        // query 4
        Arc::new({
            let orders4 = orders.clone(); 
            let lineitem4 = lineitem.clone();
            
            move || {
            let start_date = NaiveDate::from_ymd_opt(1993, 7, 1).unwrap();
            let end_date = start_date
                .checked_add_signed(chrono::Duration::days(3 * 30))
                .expect("Valid date");
            
            // query optimization
            let filtered_lineitem = lineitem4
                .clone()
                .filter(col("L_COMMITDATE").lt(col("L_RECEIPTDATE")))
                .select([col("L_ORDERKEY")]); 
            
            orders4
                .clone()
                .filter(
                    col("O_ORDERDATE").gt_eq(lit(start_date))
                        .and(col("O_ORDERDATE").lt(lit(end_date))),
                )
                .join(
                    filtered_lineitem, 
                    [col("O_ORDERKEY")],
                    [col("L_ORDERKEY")], 
                    JoinArgs::new(JoinType::Inner), 
                )
                .group_by([col("O_ORDERPRIORITY")]) 
                .agg([col("O_ORDERKEY").count().alias("order_count")]) 
                .sort(["O_ORDERPRIORITY"], SortMultipleOptions::new()
                .with_order_descending(false)) 

            }
        }),

        // query 5
        Arc::new({ 
            let customer5 = customer.clone();
            let orders5 = orders.clone();
            let lineitem5 = lineitem.clone();
            let region5 = region.clone();
            let nation5 = nation.clone();
            let supplier5 = supplier.clone();



            move || {
                let start_date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
                let end_date = start_date
                    .checked_add_signed(chrono::Duration::days(365))
                    .expect("Valid date");

                let filtered_region = region5
                    .clone()
                    .filter(col("R_NAME").eq(lit("ASIA")));

                customer5
                    .clone()
                    .join(orders5.clone(), [col("C_CUSTKEY")], [col("O_CUSTKEY")], JoinArgs::default())
                    .join(lineitem5.clone(), [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())
                    .join(supplier5.clone(), [col("L_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                    .join(nation5.clone(), [col("C_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                    .join(filtered_region, [col("N_REGIONKEY")], [col("R_REGIONKEY")], JoinArgs::default())
                    .filter(
                        col("O_ORDERDATE").gt_eq(lit(start_date))
                            .and(col("O_ORDERDATE").lt(lit(end_date))),
                    )
                    .group_by([col("N_NAME")])
                    .agg([
                        (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT")))
                            .sum()
                            .alias("REVENUE"),
                    ])
                    .sort(["REVENUE"], SortMultipleOptions::new()
                    .with_order_descending(true))
            }
        }),

        // query 6
        Arc::new({
            let lineitem6 = lineitem.clone();
    
            move || {
                let start_date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
                let end_date = start_date
                    .checked_add_signed(chrono::Duration::days(365))
                    .expect("Valid date");
    
                lineitem6
                    .clone()
                    .filter(
                        col("L_SHIPDATE").gt_eq(lit(start_date))
                            .and(col("L_SHIPDATE").lt(lit(end_date)))
                            .and(col("L_DISCOUNT").gt_eq(lit(0.06 - 0.01)).and(col("L_DISCOUNT").lt_eq(lit(0.06 + 0.01))))
                            .and(col("L_QUANTITY").lt(lit(24)))
                    )
                    .select(
                        [(col("L_EXTENDEDPRICE") * col("L_DISCOUNT"))
                            .sum()
                            .alias("REVENUE")]
                    )
            }
        }),

        // query 7
        Arc::new({
            let supplier7 = supplier.clone();
            let lineitem7 = lineitem.clone();
            let orders7 = orders.clone();
            let customer7 = customer.clone();
            let nation7 = nation.clone();
    
            move || {
                let start_date = NaiveDate::from_ymd_opt(1995, 1, 1).unwrap();
                let end_date = NaiveDate::from_ymd_opt(1996, 12, 31).unwrap();
    
                let nation71 = nation7.clone().with_column(col("N_NAME").alias("N1_NAME"));
                let nation72 = nation7.clone().with_column(col("N_NAME").alias("N2_NAME"));
    
                lineitem7
                    .clone()
                    .join(supplier7.clone(), [col("L_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                    .join(orders7.clone(), [col("L_ORDERKEY")], [col("O_ORDERKEY")], JoinArgs::default())
                    .join(customer7.clone(), [col("O_CUSTKEY")], [col("C_CUSTKEY")], JoinArgs::default())
                    .join(nation71, [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                    .join(nation72, [col("C_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                    .filter(
                        (col("N1_NAME").eq(lit("FRANCE")).and(col("N2_NAME").eq(lit("GERMANY")))
                        .or(col("N1_NAME").eq(lit("GERMANY")).and(col("N2_NAME").eq(lit("FRANCE")))))
                        .and(col("L_SHIPDATE").gt_eq(lit(start_date)))
                        .and(col("L_SHIPDATE").lt_eq(lit(end_date)))
                    )
                    .select([
                        col("N1_NAME").alias("SUPP_NATION"),
                        col("N2_NAME").alias("CUST_NATION"),
                        col("L_SHIPDATE").dt().year().alias("L_YEAR"),
                        (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT"))).alias("VOLUME"),
                    ])
                    .group_by(&[col("SUPP_NATION"), col("CUST_NATION"), col("L_YEAR")])
                    .agg([
                        col("VOLUME").sum().alias("REVENUE"),
                    ])
                    .sort(
                        ["SUPP_NATION", "CUST_NATION", "L_YEAR"],
                        SortMultipleOptions::new()
                            .with_order_descending_multi([false, false, false]),
                    )
                }
            }),

            // query 8
            Arc::new({
                let part8 = part.clone();
                let supplier8 = supplier.clone();
                let lineitem8 = lineitem.clone();
                let orders8 = orders.clone();
                let customer8 = customer.clone();
                let region8 = region.clone();
                let nation8 = nation.clone();
                let nation81 = nation8.clone().with_column(col("N_NAME").alias("N1_NAME"));
                let nation82 = nation8.clone().with_column(col("N_NAME").alias("N2_NAME"));
                
        
                move || {
                    let start_date = NaiveDate::from_ymd_opt(1995, 1, 1).unwrap();
                    let end_date = NaiveDate::from_ymd_opt(1996, 12, 31).unwrap();
        
                    // Inner query to fetch all necessary data
                    let all_nations = part8
                        .clone()
                        .join(lineitem8.clone(), [col("P_PARTKEY")], [col("L_PARTKEY")], JoinArgs::default())
                        .join(supplier8.clone(), [col("L_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                        .join(orders8.clone(), [col("L_ORDERKEY")], [col("O_ORDERKEY")], JoinArgs::default())
                        .join(customer8.clone(), [col("O_CUSTKEY")], [col("C_CUSTKEY")], JoinArgs::default())
                        .join(nation81.clone(), [col("C_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .join(region8.clone(), [col("N_REGIONKEY")], [col("R_REGIONKEY")], JoinArgs::default())
                        .join(nation82.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(
                            col("R_NAME").eq(lit("AMERICA"))
                                .and(col("O_ORDERDATE").gt_eq(lit(start_date)))
                                .and(col("O_ORDERDATE").lt_eq(lit(end_date)))
                                .and(col("P_TYPE").eq(lit("ECONOMY ANODIZED STEEL")))
                        )
                        .select([
                            col("O_ORDERDATE").dt().year().alias("O_YEAR"),
                            (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT"))).alias("VOLUME"),
                            col("N2_NAME").alias("NATION"), // use the alias from the nation72 for supplier
                        ]);
        
                    // Outer query to calculate market share
                    all_nations
                        .group_by([col("O_YEAR")])
                        .agg([
                            (when(col("NATION").eq(lit("BRAZIL")))
                                .then(col("VOLUME"))
                                .otherwise(lit(0))
                            ).sum().alias("BRAZIL_VOLUME"),
                            col("VOLUME").sum().alias("TOTAL_VOLUME"),
                        ])
                        .select([
                            col("O_YEAR"),
                            (col("BRAZIL_VOLUME") / col("TOTAL_VOLUME")).alias("MKT_SHARE"),
                        ])
                        .sort(["O_YEAR"], SortMultipleOptions::new()
                        .with_order_descending_multi([false]))
                    }
            }),

            // query 9
            Arc::new({
                let part9 = part.clone();
                let supplier9 = supplier.clone();
                let lineitem9 = lineitem.clone();
                let partsupp9 = partsupp.clone();
                let orders9 = orders.clone();
                let nation9 = nation.clone();
            
                move || {
                    // Inner query to fetch profit calculation
                    let profit = lineitem9 // Start with lineitem
                        .clone()
                        .join(supplier9.clone(), [col("L_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                        .join(partsupp9.clone(), [col("L_SUPPKEY"), col("L_PARTKEY")], [col("PS_SUPPKEY"), col("PS_PARTKEY")], JoinArgs::default())
                        .join(part9.clone(), [col("L_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .join(orders9.clone(), [col("L_ORDERKEY")], [col("O_ORDERKEY")], JoinArgs::default())
                        .join(nation9.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(col("P_NAME").str().contains(lit("green"), false))
                        .select([
                            col("N_NAME").alias("NATION"),
                            col("O_ORDERDATE").dt().year().alias("O_YEAR"),
                            (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT")) - col("PS_SUPPLYCOST") * col("L_QUANTITY")).alias("AMOUNT"),
                        ]);
            
                    // Outer query to calculate sum of profit
                    profit
                        .group_by([col("NATION"), col("O_YEAR")])
                        .agg([
                            col("AMOUNT").sum().alias("SUM_PROFIT"),
                        ])
                        .sort(["NATION", "O_YEAR"], SortMultipleOptions::new()
                        .with_order_descending_multi([false, true]))
                }
            }),


            // query 10
            Arc::new({
                let customer10 = customer.clone();
                let orders10 = orders.clone();
                let lineitem10 = lineitem.clone();
                let nation10 = nation.clone();
            
                move || {
                    // Filter for orders within the date range and with return flag 'R'
                    let start_date = NaiveDate::from_ymd_opt(1993, 10, 1).unwrap();
                    let end_date = start_date
                        .checked_add_signed(chrono::Duration::days(90)) // 3 months
                        .expect("Valid date");
            
                    customer10
                        .clone()
                        .join(orders10.clone(), [col("C_CUSTKEY")], [col("O_CUSTKEY")], JoinArgs::default())
                        .join(lineitem10.clone(), [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())
                        .join(nation10.clone(), [col("C_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(
                            col("O_ORDERDATE").gt_eq(lit(start_date))
                                .and(col("O_ORDERDATE").lt(lit(end_date)))
                                .and(col("L_RETURNFLAG").eq(lit("R")))
                        )
                        .group_by([
                            col("C_CUSTKEY"),
                            col("C_NAME"),
                            col("C_ACCTBAL"),
                            col("C_PHONE"),
                            col("N_NAME"),
                            col("C_ADDRESS"),
                            col("C_COMMENT"),
                        ])
                        .agg([
                            (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT")))
                                .sum()
                                .alias("REVENUE"),
                        ])
                        .sort(["REVENUE"], SortMultipleOptions::new()
                            .with_order_descending(true))
                }
            }),

            // query 11
            Arc::new({
                let partsupp11 = partsupp.clone();
                let supplier11 = supplier.clone();
                let nation11 = nation.clone();
            
                move || {
                    // Main query for calculating supply cost value per part
                    let subquery = partsupp11
                        .clone()
                        .join(supplier11.clone(), [col("PS_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                        .join(nation11.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(col("N_NAME").eq(lit("GERMANY")))
                        .select([col("PS_SUPPKEY").alias("PS_SUPPKEY_RIGHT"), (col("PS_SUPPLYCOST") * col("PS_AVAILQTY") * lit(0.0001)).sum().alias("THRESHOLD")]);
                    println!("{}", subquery.clone().collect().unwrap());
                    println!("{}", partsupp11.clone().collect().unwrap());
                    partsupp11
                        .clone()
                        .join(supplier11.clone(), [col("PS_SUPPKEY")], [col("S_SUPPKEY")], JoinArgs::default())
                        .join(nation11.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .join(subquery.clone(), [col("PS_SUPPKEY")], [col("PS_SUPPKEY_RIGHT")], JoinArgs::default())
                        .group_by([col("PS_PARTKEY")])
                        .agg([(col("PS_SUPPLYCOST") * col("PS_AVAILQTY")).sum().alias("VALUE"), col("THRESHOLD").first().alias("THRESHOLD")] )
                        .filter(col("VALUE").gt(col("THRESHOLD")))
                        .sort(["VALUE"], SortMultipleOptions::new().with_order_descending(true))
                        .select([col("PS_PARTKEY"), col("VALUE")])
                }
            }),

            // query 12
            Arc::new({
                let orders12 = orders.clone();
                let lineitem12 = lineitem.clone();
            
                move || {
                    let start_date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
                    let end_date = start_date
                        .checked_add_signed(chrono::Duration::days(365))
                        .expect("Valid date");
            
                    // Join `orders` and `lineitem` on `o_orderkey` = `l_orderkey`
                    orders12
                        .clone()
                        .join(lineitem12.clone(), [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())
                        .filter(
                            col("L_SHIPMODE").eq(lit("MAIL")).or(col("L_SHIPMODE").eq(lit("SHIP")))
                                .and(col("L_COMMITDATE").lt(col("L_RECEIPTDATE")))
                                .and(col("L_SHIPDATE").lt(col("L_COMMITDATE")))
                                .and(col("L_RECEIPTDATE").gt_eq(lit(start_date)))
                                .and(col("L_RECEIPTDATE").lt(lit(end_date))),
                        )
                        .group_by([col("L_SHIPMODE")])
                        .agg([
                            when(
                                col("O_ORDERPRIORITY").eq(lit("1-URGENT"))
                                    .or(col("O_ORDERPRIORITY").eq(lit("2-HIGH"))),
                            )
                            .then(lit(1))
                            .otherwise(lit(0))
                            .sum()
                            .alias("HIGH_LINE_COUNT"),
                            
                            when(
                                col("O_ORDERPRIORITY").neq(lit("1-URGENT"))
                                    .and(col("O_ORDERPRIORITY").neq(lit("2-HIGH"))),
                            )
                            .then(lit(1))
                            .otherwise(lit(0))
                            .sum()
                            .alias("LOW_LINE_COUNT"),
                        ])
                        .sort(["L_SHIPMODE"], SortMultipleOptions::new()
                            .with_order_descending(false))
                }
            }),

            // query 13
            Arc::new({
                let customer13 = customer.clone();
                let orders13 = orders.clone();
            
                move || {
                    // Step 1: Left outer join `customer` and `orders`, filtering out specific comments in `orders`
                    let customer_orders = customer13
                        .clone()
                        .join(
                            orders13
                                .clone()
                                .filter(col("O_COMMENT").str().contains(lit("special requests"), false).not()),
                            [col("C_CUSTKEY")],
                            [col("O_CUSTKEY")],
                            JoinArgs::default(),
                        )
                        .group_by([col("C_CUSTKEY")])
                        .agg([col("O_ORDERKEY").count().alias("C_COUNT")]);
            
                    // Step 2: Group by `C_COUNT` to get distribution of customers with each order count
                    customer_orders
                        .group_by([col("C_COUNT")])
                        .agg([col("C_CUSTKEY").count().alias("CUSTDIST")])
                        .sort(
                            ["CUSTDIST", "C_COUNT"],
                            SortMultipleOptions::new().with_order_descending_multi([true, true]),
                        )
                }
            }),

            // query 14
            Arc::new({
                let lineitem14 = lineitem.clone();
                let part14 = part.clone();
            
                move || {
                    // Define the date range for filtering
                    let start_date = NaiveDate::from_ymd_opt(1995, 9, 1).unwrap();
                    let end_date = start_date
                        .checked_add_signed(chrono::Duration::days(30))
                        .expect("Valid date");
            
                    // Join `lineitem` with `part` and filter for the date range
                    let filtered_data = lineitem14
                        .clone()
                        .join(part14.clone(), [col("L_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .filter(
                            col("L_SHIPDATE")
                                .gt_eq(lit(start_date))
                                .and(col("L_SHIPDATE").lt(lit(end_date))),
                        );
            
                    // Calculate total revenue and promotional revenue
                    let total_revenue = (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT"))).sum();
                    let promo_revenue = when(col("P_TYPE").str().starts_with(lit("PROMO")))
                        .then(col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT")))
                        .otherwise(lit(0.0))
                        .sum();
            
                    // Compute promo revenue as a percentage of total revenue
                    filtered_data
                        .select([
                            (promo_revenue * lit(100.0) / total_revenue)
                                .alias("PROMO_REVENUE")
                        ])
                }
            }),

            // query 15
            Arc::new({
                let lineitem15 = lineitem.clone();
                let supplier15 = supplier.clone();
            
                move || {
                    // Define the date range for filtering
                    let start_date = NaiveDate::from_ymd_opt(1996, 1, 1).unwrap();
                    let end_date = start_date
                        .checked_add_signed(chrono::Duration::days(90))
                        .expect("Valid date");
            
                    // Step 1: Calculate `total_revenue` per supplier (similar to a view in SQL)
                    let revenue_df = lineitem15
                        .clone()
                        .filter(
                            col("L_SHIPDATE")
                                .gt_eq(lit(start_date))
                                .and(col("L_SHIPDATE").lt(lit(end_date))),
                        )
                        .group_by([col("L_SUPPKEY")])
                        .agg([
                            (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT")))
                                .sum()
                                .alias("TOTAL_REVENUE"),
                        ])
                        .with_column(col("L_SUPPKEY").alias("SUPPLIER_NO"));
            
                    // Step 2: Determine the maximum `total_revenue`
                    let max_revenue = revenue_df
                        .clone()
                        .select([col("TOTAL_REVENUE").max().alias("MAX_REVENUE")])
                        .collect()
                        .expect("Selection failed");
            
                        
                    let max_revenue_value = match max_revenue.column("MAX_REVENUE") {
                        Ok(column) => match column.f64() {
                            Ok(values) => values.get(0).unwrap_or_else(|| {
                                panic!("MAX_REVENUE column exists but is empty or null");
                            }),
                            Err(_) => panic!("MAX_REVENUE column is not of type f64"),
                        },
                        Err(_) => panic!("MAX_REVENUE column does not exist"),
                    };

                    // Step 3: Join `supplier` with `revenue_df` and filter by maximum `total_revenue`
                    supplier15
                        .clone()
                        .join(revenue_df.clone(), [col("S_SUPPKEY")], [col("SUPPLIER_NO")], JoinArgs::default())
                        .filter(col("TOTAL_REVENUE").eq(lit(max_revenue_value)))
                        .select([
                            col("S_SUPPKEY"),
                            col("S_NAME"),
                            col("S_ADDRESS"),
                            col("S_PHONE"),
                            col("TOTAL_REVENUE"),
                        ])
                        .sort(["S_SUPPKEY"], SortMultipleOptions::new().with_order_descending(false))
                    
                    
                }
            }),

            // query 16
            Arc::new({
                let partsupp16 = partsupp.clone();
                let part16 = part.clone();
                let supplier16 = supplier.clone();
            
                move || {
                    // Step 1: Create `excluded_suppliers` DataFrame
                    let excluded_suppliers = supplier16
                        .clone()
                        .filter(
                            col("S_COMMENT")
                                .str()
                                .contains(lit("Customer"), false)
                                .and(col("S_COMMENT").str().contains(lit("Complaints"), false)),
                        )
                        .select([col("S_SUPPKEY")]);
                        println!("{}", excluded_suppliers.clone().collect().unwrap());
                    // Step 2: Series for `P_SIZE` filter
                    let size_values = Series::new(PlSmallStr::from_str("P_SIZE"), vec![49, 14, 23, 45, 19, 3, 36, 9]);
            
                    // Step 3: Create `valid_partsupp` with joins and filters
                    let valid_partsupp = partsupp16
                        .clone()
                        .anti_join(excluded_suppliers, col("PS_SUPPKEY"), col("S_SUPPKEY"))
                        .join(part16.clone(), [col("PS_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .filter(
                            col("P_BRAND").neq(lit("Brand#45"))
                                .and(col("P_TYPE").str().starts_with(lit("MEDIUM POLISHED")).not())
                                .and(col("P_SIZE").is_in(lit(size_values))), // Filter based on `size_values`
                        );
                        
                    // Step 4: Grouping and aggregating by required fields
                    valid_partsupp
                        .group_by([col("P_BRAND"), col("P_TYPE"), col("P_SIZE")])
                        .agg([
                            col("PS_SUPPKEY").n_unique().alias("SUPPLIER_CNT"),
                        ])
                        .sort(
                            ["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"],
                            SortMultipleOptions::new().with_order_descending_multi([true, false, false, false]),
                        )
                }
            }),
            

            // query 17
            Arc::new({
                let lineitem17 = lineitem.clone();
                let part17 = part.clone();
            
                move || {
                    // Step 1: Correlated subquery to calculate `0.2 * avg(l_quantity)` per `l_partkey`
                    let subquery = lineitem17
                        .clone()
                        .group_by([col("L_PARTKEY")])
                        .agg([
                            (col("L_QUANTITY").mean() * lit(0.2)).alias("LIMIT_QUANTITY"),
                        ]);
            
                    // Step 2: Join lineitem and part, then apply conditions on part attributes and `l_quantity`
                    lineitem17
                        .clone()
                        .join(part17.clone(), [col("L_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .join(subquery, [col("L_PARTKEY")], [col("L_PARTKEY")], JoinArgs::default())  // join to access LIMIT_QUANTITY
                        .filter(
                            col("P_BRAND").eq(lit("Brand#23"))
                            .and(col("P_CONTAINER").eq(lit("MED BOX")))
                            .and(col("L_QUANTITY").lt(col("LIMIT_QUANTITY")))  // use the correlated subquery condition
                        )
                        // Step 3: Calculate `sum(l_extendedprice) / 7.0` as `avg_yearly`
                        .select([
                            (col("L_EXTENDEDPRICE").sum() / lit(7.0)).alias("AVG_YEARLY"),
                        ])
                }
            }),
            
            // query 18
            Arc::new({
                let customer18 = customer.clone();
                let orders18 = orders.clone();
                let lineitem18 = lineitem.clone();
            
                move || {
                    // Step 1: Subquery to get `l_orderkey` where `sum(l_quantity) > 300`
                    let large_orders = lineitem18
                        .clone()
                        .group_by([col("L_ORDERKEY")])
                        .agg([
                            col("L_QUANTITY").sum().alias("TOTAL_QUANTITY")
                        ])
                        .filter(col("TOTAL_QUANTITY").gt(lit(300)))
                        .select([col("L_ORDERKEY")]);
            
                    // Step 2: Join customer, orders, and lineitem, then filter on large orders
                    customer18
                        .clone()
                        .join(orders18.clone(), [col("C_CUSTKEY")], [col("O_CUSTKEY")], JoinArgs::default())
                        .join(lineitem18.clone(), [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())
                        .join(large_orders, [col("O_ORDERKEY")], [col("L_ORDERKEY")], JoinArgs::default())  // Filter orders with sum(l_quantity) > 300
                        .group_by([
                            col("C_NAME"),
                            col("C_CUSTKEY"),
                            col("O_ORDERKEY"),
                            col("O_ORDERDATE"),
                            col("O_TOTALPRICE")
                        ])
                        .agg([
                            col("L_QUANTITY").sum().alias("TOTAL_QUANTITY"),
                        ])
                        .sort(
                            ["O_TOTALPRICE", "O_ORDERDATE"],
                            SortMultipleOptions::new().with_order_descending_multi([true, false]),
                        )
                        .select([
                            col("C_NAME"),
                            col("C_CUSTKEY"),
                            col("O_ORDERKEY"),
                            col("O_ORDERDATE"),
                            col("O_TOTALPRICE"),
                            col("TOTAL_QUANTITY")
                        ])
                }
            }),

            // query 19
            Arc::new({
                let lineitem19 = lineitem.clone();
                let part19 = part.clone();
            
                move || {
                    // Join lineitem and part on P_PARTKEY = L_PARTKEY
                    lineitem19
                        .clone()
                        .join(part19.clone(), [col("L_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .filter(
                            // Condition set 1 for Brand#12
                            (col("P_BRAND").eq(lit("Brand#12"))
                                .and(col("P_CONTAINER").is_in(lit(Series::new(PlSmallStr::from_str("containers1"), &["SM CASE", "SM BOX", "SM PACK", "SM PKG"]))))
                                .and(col("L_QUANTITY").gt_eq(lit(1)))
                                .and(col("L_QUANTITY").lt_eq(lit(11)))
                                .and(col("P_SIZE").gt_eq(lit(1)).and(col("P_SIZE").lt_eq(lit(5))))
                                .and(col("L_SHIPMODE").is_in(lit(Series::new(PlSmallStr::from_str("ship_modes1"), &["AIR", "AIR REG"]))))
                                .and(col("L_SHIPINSTRUCT").eq(lit("DELIVER IN PERSON"))))
                            // Condition set 2 for Brand#23
                            .or(
                                col("P_BRAND").eq(lit("Brand#23"))
                                    .and(col("P_CONTAINER").is_in(lit(Series::new(PlSmallStr::from_str("containers2"), &["MED BAG", "MED BOX", "MED PKG", "MED PACK"]))))
                                    .and(col("L_QUANTITY").gt_eq(lit(10)))
                                    .and(col("L_QUANTITY").lt_eq(lit(20)))
                                    .and(col("P_SIZE").gt_eq(lit(1)).and(col("P_SIZE").lt_eq(lit(10))))
                                    .and(col("L_SHIPMODE").is_in(lit(Series::new(PlSmallStr::from_str("ship_modes2"), &["AIR", "AIR REG"]))))
                                    .and(col("L_SHIPINSTRUCT").eq(lit("DELIVER IN PERSON")))
                            )
                            // Condition set 3 for Brand#34
                            .or(
                                col("P_BRAND").eq(lit("Brand#34"))
                                    .and(col("P_CONTAINER").is_in(lit(Series::new(PlSmallStr::from_str("containers3"), &["LG CASE", "LG BOX", "LG PACK", "LG PKG"]))))
                                    .and(col("L_QUANTITY").gt_eq(lit(20)))
                                    .and(col("L_QUANTITY").lt_eq(lit(30)))
                                    .and(col("P_SIZE").gt_eq(lit(1)).and(col("P_SIZE").lt_eq(lit(15))))
                                    .and(col("L_SHIPMODE").is_in(lit(Series::new(PlSmallStr::from_str("ship_modes3"), &["AIR", "AIR REG"]))))
                                    .and(col("L_SHIPINSTRUCT").eq(lit("DELIVER IN PERSON")))
                            )
                        )
                        // Aggregate to get the revenue
                        .select([
                            (col("L_EXTENDEDPRICE") * (lit(1.0) - col("L_DISCOUNT"))).sum().alias("REVENUE"),
                        ])
                }
            }),
            
            // query 20
            Arc::new({
                let supplier20 = supplier.clone();
                let nation20 = nation.clone();
                let partsupp20 = partsupp.clone();
                let part20 = part.clone();
                let lineitem20 = lineitem.clone();
            
                move || {
                    // Step 1: Filter `part` for parts with names starting with "forest"
                    let start_date = NaiveDate::from_ymd_opt(1994, 1, 1).unwrap();
                    let end_date = start_date
                        .checked_add_signed(chrono::Duration::days(365))
                        .expect("Valid date");

                    let forest_parts = part20
                        .clone()
                        .filter(col("P_NAME").str().contains(lit("forest"), false))
                        .select([col("P_PARTKEY")]);
            
                    // Step 2: Calculate `0.5 * sum(l_quantity)` for each matching `ps_partkey` and `ps_suppkey`
                    let quantity_threshold = lineitem20
                        .clone()
                        .join(partsupp20.clone(), [col("L_PARTKEY")], [col("PS_PARTKEY")], JoinArgs::default())
                        .filter(
                            col("L_SHIPDATE")
                                .gt_eq(lit(start_date))
                                .and(col("L_SHIPDATE").lt(lit(end_date))),
                        )
                        .group_by([col("L_PARTKEY"), col("L_SUPPKEY")])
                        .agg([(col("L_QUANTITY").sum() * lit(0.5)).alias("QUANTITY_THRESHOLD")]);
            
                    // Step 3: Filter `partsupp` with availability quantity greater than `quantity_threshold`
                    let qualifying_partsupp = partsupp20
                        .clone()
                        .join(forest_parts, [col("PS_PARTKEY")], [col("P_PARTKEY")], JoinArgs::default())
                        .join(quantity_threshold, [col("PS_PARTKEY"), col("PS_SUPPKEY")], [col("L_PARTKEY"), col("L_SUPPKEY")], JoinArgs::default())
                        .filter(col("PS_AVAILQTY").gt(col("QUANTITY_THRESHOLD")))
                        .select([col("PS_SUPPKEY")]);
            
                    // Step 4: Filter `supplier` for those matching the `qualifying_partsupp` and join with `nation`
                    supplier20
                        .clone()
                        .join(qualifying_partsupp, [col("S_SUPPKEY")], [col("PS_SUPPKEY")], JoinArgs::default())
                        .join(nation20.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(col("N_NAME").eq(lit("CANADA")))
                        .select([col("S_NAME"), col("S_ADDRESS")])
                        .sort(["S_NAME"], SortMultipleOptions::new().with_order_descending_multi([false]))
                }
            }),
            
            // query 21
            Arc::new({
                let supplier21 = supplier.clone();
                let lineitem21 = lineitem.clone();
                let orders21 = orders.clone();
                let nation21 = nation.clone();
            
                move || {

                    let l1 = lineitem21.clone().select([
                        col("L_SUPPKEY").alias("L1_SUPPKEY"), 
                        col("L_ORDERKEY").alias("L1_ORDERKEY"), 
                        col("L_RECEIPTDATE").alias("L1_RECEIPTDATE"), 
                        col("L_COMMITDATE").alias("L1_COMMITDATE")]);
                    let l2 = lineitem21.clone().select([
                        col("L_SUPPKEY").alias("L2_SUPPKEY"), 
                        col("L_ORDERKEY").alias("L2_ORDERKEY")]);
                    let l3 = lineitem21.clone().select([
                        col("L_SUPPKEY").alias("L3_SUPPKEY"), 
                        col("L_ORDERKEY").alias("L3_ORDERKEY"), 
                        col("L_RECEIPTDATE").alias("L3_RECEIPTDATE"), 
                        col("L_COMMITDATE").alias("L3_COMMITDATE")]);

                    let all_l1 = supplier21
                        .clone()
                        .join(l1.clone(), [col("S_SUPPKEY")], [col("L1_SUPPKEY")], JoinArgs::default())
                        //.join(l3.clone(), [col("L_ORDERKEY")], [col("O_ORDERKEY")], JoinArgs::default())
                        .join(orders21.clone(), [col("L1_ORDERKEY")], [col("O_ORDERKEY")], JoinArgs::default())
                        .join(nation21.clone(), [col("S_NATIONKEY")], [col("N_NATIONKEY")], JoinArgs::default())
                        .filter(col("O_ORDERSTATUS").eq(lit("F"))
                            .and(col("N_NAME").eq(lit("SAUDI ARABIA")))
                            .and(col("L1_RECEIPTDATE").gt("L1_COMMITDATE")));
                    
                    let not_exist = l3
                        .clone()
                        .join(all_l1.clone(), [col("L3_ORDERKEY")], [col("L1_ORDERKEY")], JoinArgs::default())
                        .filter(col("L3_SUPPKEY").neq(col("S_SUPPKEY"))
                                .and(col("L3_RECEIPTDATE").gt(col("L3_COMMITDATE"))));
                    
                    all_l1
                        .clone()
                        .anti_join(not_exist.clone(), col("L1_ORDERKEY"), col("L3_ORDERKEY"))
                        .join(l2.clone(), [col("L1_ORDERKEY")], [col("L2_ORDERKEY")], JoinArgs::default())
                        .filter(col("L2_SUPPKEY").neq(col("S_SUPPKEY")))
                        .group_by([col("S_NAME")])
                        .agg([col("L1_ORDERKEY").count().alias("NUMWAIT")])
                        .sort(
                            ["NUMWAIT", "S_NAME"],
                            SortMultipleOptions::new().with_order_descending_multi([true, false]),
                        )
                }
            }),
            
            
            Arc::new({
                let customer22 = customer.clone();
                let orders22 = orders.clone();
            
                move || {
                    // Step 1: Calculate average account balance for the specific country codes
                    let country_codes = Series::new(PlSmallStr::from_str("country_codes"), &["13", "31", "23", "29", "30", "18", "17"]);
                    
                    let not_exist = orders22
                        .clone()
                        .join(customer22.clone(), [col("O_CUSTKEY")], [col("C_CUSTKEY")], JoinArgs::default());
                    
                    let avg_acctbal = customer22
                        .clone()
                        .filter(
                            col("C_ACCTBAL").gt(lit(0.00))
                                .and(col("C_PHONE").str().slice(lit(0), lit(2)).is_in(lit(country_codes.clone()))),
                        )
                        .select([col("C_ACCTBAL").alias("C_ACCTBAL_AVG")])
                        .mean()
                        .collect()
                        .unwrap()
                        .column("C_ACCTBAL_AVG")
                        .unwrap()
                        .f64()
                        .unwrap()
                        .get(0)
                        .unwrap();
                    
                    // Step 2: Filter customers based on the criteria
                    let custsale = customer22
                        .clone()
                        .filter(
                            col("C_PHONE").str().slice(lit(0), lit(2)).is_in(lit(country_codes.clone()))
                                .and(col("C_ACCTBAL").gt(lit(avg_acctbal)))
                        )
                        .anti_join(not_exist.clone(), col("C_CUSTKEY"), col("O_CUSTKEY"))
                        .select([col("C_PHONE"), col("C_ACCTBAL"), col("C_CUSTKEY")]);
            
                    // Step 3: Left join with orders to filter out customers with existing orders
                    custsale
                        .clone()
                        .with_columns([col("C_PHONE").str().slice(lit(0), lit(2)).alias("CNTRYCODE")])
                        .group_by([col("CNTRYCODE")])
                        .agg([
                            col("C_ACCTBAL").count().alias("NUMCUST"),
                            col("C_ACCTBAL").sum().alias("TOTACCTBAL"),
                        ])
                        .sort(["CNTRYCODE"], SortMultipleOptions::new().with_order_descending_multi([false]))
                }
            }),

        

    ];

    // query to be checked: 11,16,21,22
    //execute_query_at_index(queries, 16);
    execute_all_query(queries);



    


    // write the result back to a CSV file
    //let mut file = std::fs::File::create(format!("{}query1.csv", output_path))?;
    //let mut df = result.collect()?;
    //CsvWriter::new(&mut file)
    //    .include_header(true)
    //    .finish(&mut df)?;
    
    Ok(())
}
