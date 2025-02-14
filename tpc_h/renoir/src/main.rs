
use std::{error::Error, fs::create_dir_all};
use std::env;
use regex::Regex;
use serde_json::json;
use renoir::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use chrono::{Datelike, NaiveDate};
use ordered_float::OrderedFloat;
use csv::Writer;


#[derive(Clone, Deserialize, Serialize, Debug)]
struct PartRow {
    p_partkey: i32,
    p_name: String,
    p_mfgr: String,
    p_brand: String,
    p_type: String,
    p_size: i32,
    p_container: String,
    p_retailprice: f64,
    p_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct SupplierRow {
    s_suppkey: i32,
    s_name: String,
    s_address: String,
    s_nationkey: i32,
    s_phone: String,
    s_acctbal: f64,
    s_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct PartsuppRow {
    ps_partkey: i32,
    ps_suppkey: i32,
    ps_availqty: i32,
    ps_supplycost: f64,
    ps_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct CustomerRow {
    c_custkey: i32,
    c_name: String,
    c_address: String,
    c_nationkey: i32,
    c_phone: String,
    c_acctbal: f64,
    c_mktsegment: String,
    c_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct OrdersRow {
    o_orderkey: i32,
    o_custkey: i32,
    o_orderstatus: String,
    o_totalprice: f64,
    o_orderdate: String,
    o_orderpriority: String,
    o_clerk: String,
    o_shippriority: i32,
    o_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct LineitemRow {
    l_orderkey: i32,
    l_partkey: i32,
    l_suppkey: i32,
    l_linenumber: i32,
    l_quantity: f64,
    l_extendedprice: f64,
    l_discount: f64,
    l_tax: f64,
    l_returnflag: String,
    l_linestatus: String,
    l_shipdate: String,
    l_commitdate: String,
    l_receiptdate: String,
    l_shipinstruct: String,
    l_shipmode: String,
    l_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct NationRow {
    n_nationkey: i32,
    n_name: String,
    n_regionkey: i32,
    n_comment: String,
}
#[derive(Clone, Deserialize, Serialize, Debug)]
struct RegionRow {
    r_regionkey: i32,
    r_name: String,
    r_comment: String,
}

// query 1
fn query1(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum_qty: f64,
        sum_base_price: f64,
        sum_disc_price: f64,
        sum_charge: f64,
        sum_qty_for_avg: f64,
        sum_price_for_avg: f64,
        sum_discount_for_avg: f64,
        count_order: u64,
    }



    // initialization
    let ctx = StreamContext::new(config);
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<i64>().unwrap().clone();
    let filter_date = NaiveDate::parse_from_str("1998-12-01", "%Y-%m-%d")
        .unwrap()
        - chrono::Duration::days(param0);

    let res = lineitem
        .filter(move |row| {
            // Parse the `l_shipdate` string to a `NaiveDate` for comparison
            let ship_date = NaiveDate::parse_from_str(&row.l_shipdate, "%Y-%m-%d").unwrap();
            ship_date <= filter_date
        })
        .group_by_fold(
        |row| (row.l_returnflag.clone(), row.l_linestatus.clone()), // Key extraction
        Aggregates::default(),                                      // Initial accumulator
        |acc, row| {
            acc.sum_qty += row.l_quantity;
            acc.sum_base_price += row.l_extendedprice;
            acc.sum_disc_price += row.l_extendedprice * (1.0 - row.l_discount);
            acc.sum_charge += row.l_extendedprice 
                              * (1.0 - row.l_discount) 
                              * (1.0 + row.l_tax);
    
            
            acc.sum_qty_for_avg += row.l_quantity;
            acc.sum_price_for_avg += row.l_extendedprice;
            acc.sum_discount_for_avg += row.l_discount;
    
    
            acc.count_order += 1;
        },
        |acc, other| {
            acc.sum_qty += other.sum_qty;
            acc.sum_base_price += other.sum_base_price;
            acc.sum_disc_price += other.sum_disc_price;
            acc.sum_charge += other.sum_charge;
    
            acc.sum_qty_for_avg += other.sum_qty_for_avg;
            acc.sum_price_for_avg += other.sum_price_for_avg;
            acc.sum_discount_for_avg += other.sum_discount_for_avg;
            acc.count_order += other.count_order;
        },
    ).unkey()
    .map(|((l_returnflag, l_linestatus), acc)| {
        let mut acc = acc;
        acc.sum_qty_for_avg /= acc.count_order as f64;
        acc.sum_price_for_avg /= acc.count_order as f64;
        acc.sum_discount_for_avg /= acc.count_order as f64;

        (   
            l_returnflag,
            l_linestatus,
            acc.sum_qty,
            acc.sum_base_price,
            acc.sum_disc_price,
            acc.sum_charge,
            acc.sum_qty_for_avg,
            acc.sum_price_for_avg,
            acc.sum_discount_for_avg,
            acc.count_order
        )
    }).collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0)
            .then_with(|| v1.1.cmp(&v2.1))
    });

    let mut wtr = Writer::from_path(format!("{}/query1.csv", output_path)).unwrap();
    wtr.write_record(&[
        "l_returnflag",
        "l_linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count_order",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }

    wtr.flush().unwrap();
}

// query 2
fn query2(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let mut supplier = ctx.stream(supplier_source).split(2);
    let mut partsupp = ctx.stream(partsupp_source).split(2);
    let mut nation = ctx.stream(nation_source).split(2);
    let mut region = ctx.stream(region_source).split(2);

    let param0 = params[0].parse::<i32>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    let param21 = params[2].parse::<String>().unwrap().clone();
    let param22 = params[2].parse::<String>().unwrap().clone();
        // Find the minimum ps_supplycost for each p_partkey in the "EUROPE" region
    let min_supply_cost = partsupp.pop().unwrap()
        .join(supplier.pop().unwrap(), |ps_row| ps_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation.pop().unwrap(), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(region.pop().unwrap().filter(move |r_row| r_row.r_name == param21), |(_, ((_,(_, _)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey)
        .unkey()
        .map(|row: (i32, ((i32, ((i32, (PartsuppRow, SupplierRow)), NationRow)), RegionRow))| (row.1.0.1.0.1.0.ps_partkey, row.1.0.1.0.1.0.ps_supplycost))
        .group_by_min_element(                                              
           |(ps_key, _)| ps_key.to_owned(),           
           |(_, ps_cost)| OrderedFloat(ps_cost.to_owned()),
        )
        .drop_key();
        //.inspect(|row| println!("Item: {:?}", row)).for_each(std::mem::drop);  // (i32 ps_partkey, f64 mis_supply_cost) 
    

    let res = part
        .filter(move |p_row| 
            (p_row.p_size == param0) && (p_row.p_type.contains(&param1))
        )
        .join(partsupp.pop().unwrap(), |p_row| p_row.p_partkey, |ps_row| ps_row.ps_partkey)
        .unkey()
        .join(supplier.pop().unwrap(), |(_, (_, ps_row))| ps_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation.pop().unwrap(), |(_, ((_, (_, _)), s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(region.pop().unwrap().filter(move |r_row| r_row.r_name == param22), |(_, ((_, ((_, (_, _)), _)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey )
        .unkey()
        .join(min_supply_cost, |(_, ((_, ((_, ((_, (_, ps_row)), _)), _)), _))| (ps_row.ps_partkey, OrderedFloat(ps_row.ps_supplycost)), |min_ps| (min_ps.0, OrderedFloat(min_ps.1)))
        .unkey()
      //let _: () = inter;  
        .map(|((_, _ps_min_supp_cost), ((_r_key, ((_s_n_key, ((_ps_suppkey, ((_ps_partkey, (p_row, _ps_row)), s_row)), n_row)), _r_row)), (_, _)))|  (s_row.s_acctbal, s_row.s_name, n_row.n_name, p_row.p_partkey, p_row.p_mfgr, s_row.s_address, s_row.s_phone, s_row.s_comment))
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        OrderedFloat(v2.0).cmp(&OrderedFloat(v1.0))// s_acctbal desc
            .then_with(|| v1.1.cmp(&v2.1)) // s_name
            .then_with(|| v1.2.cmp(&v2.2)) // n_name
            .then_with(|| v1.3.cmp(&v2.3)) // p_partkey
    });

    let mut wtr = Writer::from_path(format!("{}/query2.csv", output_path)).unwrap();
    wtr.write_record(&[
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mmfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }
}


// query 3
fn query3(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);


    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param11 = params[1].parse::<String>().unwrap().clone();
    let param12 = params[1].parse::<String>().unwrap().clone();

    let res = customer
        .filter(move |c_row| c_row.c_mktsegment == param0)
        .join(orders.filter(move |o_row| {
            let o_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let filter_date = NaiveDate::parse_from_str(param11.as_str(), "%Y-%m-%d").unwrap();
            o_date < filter_date}), 
            |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        //.map(|row: (i32, (CustomerRow, OrdersRow))| row.1)
        // c_custkey, c_row, o_row
        .join(lineitem.filter(move |l_row| {
            let l_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let filter_date = NaiveDate::parse_from_str(param12.as_str(), "%Y-%m-%d").unwrap();
            l_date > filter_date}) 
            , |(_,(_, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        //.map(|row: (i32, ((CustomerRow, OrdersRow), LineitemRow))| row.1)
        .group_by_sum(|(_, ((_, (_, o_row)), l_row))| (l_row.l_orderkey, o_row.o_orderdate.clone(), o_row.o_shippriority), 
                        |(_, ((_, (_, _)), l_row))| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        // todo.map()
        .unkey()
        .map(|row| (row.0.0, row.1, row.0.1, row.0.2))// l_orderkey, revenue, o_orderdate, o_shippriority, 
        .collect_vec();
        // todo non ho capito perchè non funziona questa seconda parte 
    ctx.execute_blocking();
    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        OrderedFloat(v2.1).cmp(&OrderedFloat(v1.1))// revenue desc
            .then_with(|| 
                {
                    let v1_odate = NaiveDate::parse_from_str(v1.2.as_str(), "%Y-%m-%d").unwrap();
                    let v2_odate = NaiveDate::parse_from_str(v2.2.as_str(), "%Y-%m-%d").unwrap();
                    v1_odate.cmp(&v2_odate)
                }) // o_orderdate
    });

    let mut wtr = Writer::from_path(format!("{}/query3.csv", output_path)).unwrap();
    wtr.write_record(&[
        "l_orderkey",
        "revenue",
        "o_orderdate",
        "o_shippriority",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
        }   

}

// query 4
fn query4(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);

    let mut orders = ctx.stream(orders_source).split(2);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();

    let exist_query = lineitem
        .filter(|l_row| {
            let commit_date = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();
            let receipt_date = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            commit_date < receipt_date
        })
        .join(orders.pop().unwrap(), |l_row| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .map(|(_, (_, o_row))| o_row.o_orderkey)
        .rich_filter_map({
            let mut seen_keys = std::collections::HashSet::new();
            move |order_key| {
                if seen_keys.insert(order_key) {
                    Some(order_key)
                } else {
                    None
                }
            }
        });
    
    let res = orders.pop().unwrap()
        .filter(move |o_row| {
            let start_date = NaiveDate::parse_from_str(param0.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chronoutil::RelativeDuration::months(3);
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            (order_date >= start_date) && (order_date < end_date)
        })
        .join(exist_query, |o_row| o_row.o_orderkey, |order_key| *order_key)
        .unkey()
        .group_by_count(|(_, (o_row, _))| o_row.o_orderpriority.to_string())
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0)
    });

    let mut wtr = Writer::from_path(format!("{}/query4.csv", output_path)).unwrap();
    wtr.write_record(&[
        "o_orderpriority",
        "order_count",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }   


}


// query 5
fn query5(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);

    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path)).has_headers(false);


    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let region = ctx.stream(region_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    
    let res = customer
        .join(orders
            .filter(move |o_row| {
                let start_date = NaiveDate::parse_from_str(param1.as_str(), "%Y-%m-%d").unwrap();
                let end_date = start_date + chrono::Duration::days(365);
                let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();

                (order_date >= start_date) && (order_date < end_date)
            }), 
            |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        .join(lineitem, |(_, (_c_row, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .join(supplier, |(_, ((_, (_, _o_row)), l_row))| l_row.l_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .filter(|(_, ((_, ((_, (c_row, _)), _)), s_row))| c_row.c_nationkey == s_row.s_nationkey )
        .join(nation, |(_, ((_, ((_, (_c_row, _o_row)), _l_row)), s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()         
        .join(region.filter(move |r_row| r_row.r_name.to_string().eq(param0.as_str())), |(_, ((_, ((_, ((_, (_c_row, _o_row)), _l_row)), _s_row)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey)
        .unkey()
        .map(|(_, ((_, ((_, ((_, ((_, (_c_row, _o_row)), l_row)), _s_row)), n_row)), _r_row))| (n_row.n_name, l_row.l_extendedprice, l_row.l_discount))
        .group_by_sum(|(n_name, _, _)| n_name.to_string(), |(_, l_extendeprice, l_discount)| l_extendeprice * (1.0 - l_discount))
        .unkey()
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        OrderedFloat(v2.1).cmp(&OrderedFloat(v1.1))
    });

    let mut wtr = Writer::from_path(format!("{}/query5.csv", output_path)).unwrap();
    wtr.write_record(&[
        "n_name",
        "revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }   
    
    

}

// query 6
fn query6(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<f64>().unwrap().clone();
    let param2 = params[2].parse::<f64>().unwrap().clone();

    let res = lineitem
        .filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(param0.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(365);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            (ship_date >= start_date) && (ship_date < end_date) 
            && (l_row.l_discount >= param1-0.01) && (l_row.l_discount <= param1+0.01)
            && (l_row.l_quantity < param2)
        })
        .fold_assoc(0.0, |sum, l_row| *sum += l_row.l_extendedprice * l_row.l_discount, |sum1, sum2| *sum1 += sum2)
        .collect_vec();

    ctx.execute_blocking();
    
    let res = res.get().unwrap();

    let mut wtr = Writer::from_path(format!("{}/query6.csv", output_path)).unwrap();
    wtr.write_record(&[
        "revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }   
    
}

// query 7
fn query7(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {


    let ctx = StreamContext::new(config);

    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);


    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let mut nation = ctx.stream(nation_source).split(2);
    

    let nation1 = nation.pop().unwrap();
    let nation2 = nation.pop().unwrap();

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();

    
    let shipping = supplier
        .join(lineitem, |s_row| s_row.s_suppkey, |l_row| l_row.l_suppkey)
        .unkey()
        .join(orders, |(_, (_, l_row))| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .join(customer, |(_, ((_, (_, _)), o_row))| o_row.o_custkey, |c_row| c_row.c_custkey)
        .unkey()
        .join(nation1, |(_, ((_, ((_, (s_row, _)), _)), _))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(nation2, |(_, ((_, ((_, ((_, (_, _)), _)), c_row)), _))| c_row.c_nationkey, |n_row| n_row.n_nationkey)
        .filter(move |(_, ((_, ((_, ((_, ((_, (_, l_row)), _)), _)), n1_row)), n2_row))| {
            let start_date = NaiveDate::parse_from_str("1995-01-01", "%Y-%m-%d").unwrap();
            let end_date = NaiveDate::parse_from_str("1996-12-31", "%Y-%m-%d").unwrap();
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            
            ((n1_row.n_name == param0) && (n2_row.n_name == param1)
            || (n1_row.n_name == param1) && (n2_row.n_name == param0))
            && (ship_date >= start_date && ship_date <= end_date)
        })
        .unkey()
        .map(|(_, ((_, ((_, ((_, ((_, (_, l_row)), _)), _)), n1_row)), n2_row))| {
            let l_shipdate = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let l_year = l_shipdate.year().to_string();
            (n1_row.n_name, n2_row.n_name, l_year, l_row.l_extendedprice * (1.0 - l_row.l_discount))
        });
    
    let res = shipping
        .group_by_sum(|(supp_nation, cust_nation, l_year, _)| (supp_nation.clone(), cust_nation.clone(), l_year.clone()), |(_, _, _, volume)| volume)
        .unkey()
        .map(|((supp_nation, cust_nation, l_year), revenue)| (supp_nation, cust_nation, l_year, revenue))
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // supp_nation
            .then_with(|| v1.1.cmp(&v2.1)) // cust_nation
            .then_with(|| v1.2.cmp(&v2.2)) // l_year
    });

    let mut wtr = Writer::from_path(format!("{}/query7.csv", output_path)).unwrap();
    wtr.write_record(&[
        "supp_nation",
        "cust_nation",
        "l_year",
        "revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }   


}


// query 8
fn query8(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum_nation : f64,
        sum_tot: f64
    }

    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let mut nation = ctx.stream(nation_source).split(2);
    let region = ctx.stream(region_source);

    let nation1 = nation.pop().unwrap();
    let nation2 = nation.pop().unwrap();

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    let param2 = params[2].parse::<String>().unwrap().clone();

    let all_nations = part
        .filter(move |p_row| p_row.p_type == param2)
        .join(lineitem, |p_row| p_row.p_partkey, |l_row| l_row.l_partkey)
        .drop_key()
        .join(supplier, |(_, l_row)| l_row.l_suppkey, |s_row| s_row.s_suppkey)
        .drop_key()
        .join(orders.filter(|o_row| {
            let start_date = NaiveDate::parse_from_str("1995-01-01", "%Y-%m-%d").unwrap();
            let end_date = NaiveDate::parse_from_str("1996-12-31", "%Y-%m-%d").unwrap();
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();

            order_date >= start_date && order_date <= end_date
        }), |((_,l_row), _)| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .drop_key()
        .join(customer, |(((_, _),_), o_row)| o_row.o_custkey, |c_row| c_row.c_custkey)
        .drop_key()
        .join(nation1, |((((_, _), _), _), c_row)| c_row.c_nationkey, |n1_row| n1_row.n_nationkey)
        .drop_key()
        .join(nation2, |(((((_, _), s_row), _), _), _)| s_row.s_nationkey, |n2_row| n2_row.n_nationkey)
        .drop_key()
        .join(region.filter(move |r_row| r_row.r_name == param1), |((((((_, _), _), _), _), n1_row), _)| n1_row.n_regionkey, |r_row| r_row.r_regionkey)
        .drop_key()
        .map(|(((((((_, l_row), _), o_row), _), _), n2_row), _)| {
            let o_orderdate = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let o_year = o_orderdate.year().to_string();

            (o_year, l_row.l_extendedprice * (1.0 - l_row.l_discount), n2_row.n_name)
        });
        
    
    let res = all_nations
        .group_by_fold(|(o_year, _, _)| o_year.clone(), Aggregates::default(), move |acc, (_, volume, nation)| {
            if nation == param0 {
                acc.sum_nation += volume;
            }
            acc.sum_tot += volume;
        },
            |acc, other| {
                acc.sum_nation += other.sum_nation;
                acc.sum_tot += other.sum_tot;
            }
        )
        .unkey()
        .map(|(o_year, acc)| (o_year.clone(), acc.sum_nation/acc.sum_tot))
        .collect_vec();
        
    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        let v1_oyear  : i32 = v1.0.parse().unwrap();
        let v2_oyear  : i32 = v2.0.parse().unwrap();
        
        v1_oyear.cmp(&v2_oyear) // o_year asc
    });

    let mut wtr = Writer::from_path(format!("{}/query8.csv", output_path)).unwrap();
    wtr.write_record(&[
        "o_year",
        "mkt_share",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }   
    

    
}


// query 9
fn query9(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let partsupp = ctx.stream(partsupp_source);

    let param0 = params[0].parse::<String>().unwrap().clone();

    let profit = lineitem
        .join(supplier, |l_row| (l_row.l_suppkey), |s_row| s_row.s_suppkey)
        .drop_key()
        .join(partsupp, |(l_row, _)| (l_row.l_suppkey, l_row.l_partkey), |ps_row|(ps_row.ps_suppkey, ps_row.ps_partkey))
        .drop_key()
        .join(part.filter(move |p_row| p_row.p_name.contains(param0.as_str())), |((l_row,_),_)| l_row.l_partkey, |p_row| p_row.p_partkey)
        .drop_key()
        .join(orders, |(((l_row,_),_),_)| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .drop_key()
        .join(nation, |((((_,s_row),_),_),_)| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .drop_key()
        .map(|(((((l_row,_),ps_row),_),o_row),n_row)| {
            let o_orderdate = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let o_year = o_orderdate.year().to_string();
            (n_row.n_name, o_year, l_row.l_extendedprice * (1.0 - l_row.l_discount) - ps_row.ps_supplycost * l_row.l_quantity)
        });

    let res = profit
        .group_by_sum(|(nation, o_year, _)| (nation.clone(), o_year.clone()), |(_, _, amount)| amount)
        .unkey()
        .map(|((nation, o_year), sum_profit)| (nation, o_year, sum_profit))
        .collect_vec();
    
    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // nation
            .then_with(|| {
        let v1_oyear : i32 = v1.1.parse().unwrap();
        let v2_oyear : i32 = v2.1.parse().unwrap();

        v2_oyear.cmp(&v1_oyear)}) // o_year desc
    });

    let mut wtr = Writer::from_path(format!("{}/query9.csv", output_path)).unwrap();
    wtr.write_record(&[
        "nation",
        "o_year",
        "sum_profit",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
}


// query 10
fn query10(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    let ctx = StreamContext::new(config);

    
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);

    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let customer = ctx.stream(customer_source);

    let param0 = params[0].parse::<String>().unwrap().clone();

    let res = customer
        .join(orders.filter(move |o_row| {
            let start_date = NaiveDate::parse_from_str(&param0.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chronoutil::RelativeDuration::months(3);
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();

            order_date >= start_date && order_date < end_date
        }), |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .drop_key()
        .join(lineitem.filter(|l_row| l_row.l_returnflag == "R"), |(_, o_row)| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .drop_key()
        .join(nation, |((c_row,_),_)| c_row.c_nationkey, |n_row| n_row.n_nationkey)
        .drop_key()
        .group_by_sum(|(((c_row,_),_),n_row)| (c_row.c_custkey, c_row.c_name.clone(), c_row.c_acctbal as u64, n_row.n_name.clone(), c_row.c_address.clone(), c_row.c_phone.clone(), c_row.c_comment.clone()), 
            |(((_,_),l_row),_)| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .unkey()
        .map(|((c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment), revenue)| (c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment))
        .collect_vec();


    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        OrderedFloat(v2.2).cmp(&OrderedFloat(v1.2)) // revenue
    });

    let mut wtr = Writer::from_path(format!("{}/query10.csv", output_path)).unwrap();
    wtr.write_record(&[
        "c_custkey",
        "c_name",
        "revenue",
        "c_acctbal",
        "n_name",
        "c_address",
        "c_phone",
        "c_comment",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
}


// query 11
fn query11(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    let ctx = StreamContext::new(config.clone());

    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    
    let partsupp = ctx.stream(partsupp_source.clone());
    let supplier = ctx.stream(supplier_source.clone());
    let nation = ctx.stream(nation_source.clone());
    
    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<f64>().unwrap().clone();

    let threshold = 
        partsupp
            .join(supplier, |p_row| p_row.ps_suppkey, |s_row| s_row.s_suppkey)
            .unkey()
            .join(nation.filter(move |n_row| n_row.n_name == param0.as_str()), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
            .unkey()
            .fold_assoc(0.0, |sum, (_, ((_, (ps_row, _)), _))| *sum += ps_row.ps_supplycost * ps_row.ps_availqty as f64, |sum, other| *sum += other)
            .map(move |sum| sum * param1)
            .collect::<Vec<f64>>();
            
    
    ctx.execute_blocking();

    let threshold_value = threshold
        .get()
        .unwrap()
        .get(0)
        .cloned()
        .unwrap();
    

    let ctx2 = StreamContext::new(config.clone());
    let partsupp2 = ctx2.stream(partsupp_source.clone());
    let supplier2 = ctx2.stream(supplier_source.clone());
    let nation2  = ctx2.stream(nation_source.clone());
    
    let param01 = params[0].parse::<String>().unwrap().clone();

    let res = partsupp2
        .join(supplier2, |p_row| p_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation2.filter(move |n_row| n_row.n_name == param01.as_str()), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .group_by_sum(
            |(_, ((_, (ps_row, _)), _))| ps_row.ps_partkey, // Grouping by part key
            |(_, ((_, (ps_row, _)), _))| ps_row.ps_supplycost * ps_row.ps_availqty as f64,
        )
        .unkey()
        .filter(move |(_, value)| *value > threshold_value) 
        .collect_vec();

    ctx2.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        OrderedFloat(v2.1).cmp(&OrderedFloat(v1.1)) // value
    });

    let mut wtr = Writer::from_path(format!("{}/query11.csv", output_path)).unwrap();
    wtr.write_record(&[
        "ps_partkey",
        "value",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
}


// query 12
fn query12(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        high_line_count : i32,
        low_line_count : i32
    }

    let ctx = StreamContext::new(config);

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);

    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    let param2 = params[2].parse::<String>().unwrap().clone();

    let res = orders
        .join(lineitem.filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(param2.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chronoutil::RelativeDuration::years(1);
            let receipt_date = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let commit_date = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();
            
            (l_row.l_shipmode == param0.as_str() ||  l_row.l_shipmode == param1.as_str()) 
            && (receipt_date >= start_date && receipt_date <= end_date)
            && (commit_date < receipt_date && ship_date < commit_date)
        }), |o_row| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .drop_key()
        .group_by_fold(|(_, l_row)| l_row.l_shipmode.clone(), Aggregates::default(), 
            |acc, (o_row, _)| {
                if o_row.o_orderpriority == "1-URGENT" || o_row.o_orderpriority == "2-HIGH" {
                    acc.high_line_count += 1;
                }
                else if o_row.o_orderpriority != "1-URGENT" && o_row.o_orderpriority != "2-HIGH" {
                    acc.low_line_count += 1;
                }
            }, 
            |acc, other| {
                acc.high_line_count += other.high_line_count;
                acc.low_line_count += other.low_line_count;
            }
        )
        .unkey()
        .map(|(l_shipmode, acc)| (l_shipmode, acc.high_line_count, acc.low_line_count))
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // l_shipmode
    });

    let mut wtr = Writer::from_path(format!("{}/query12.csv", output_path)).unwrap();
    wtr.write_record(&[
        "l_shipmode",
        "high_line_count",
        "low_line_count",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
    
}

// query 13
fn query13(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config);

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    
    let orders = ctx.stream(orders_source);
    let customer = ctx.stream(customer_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();

    let pattern = format!("{}.*{}", regex::escape(&param0), regex::escape(&param1));
    let re = Regex::new(&pattern).unwrap();
    
    let c_orders = customer
        .left_join(orders.filter(move |o_row| !re.is_match(&o_row.o_comment)),|c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .drop_key()
        .map(|(c_row, order)| (c_row.c_custkey, order.is_some() as i32)) 
        .group_by_sum(|(custkey, _)| *custkey, |(_, count)| count)
        .unkey();

    let  res = c_orders
        .group_by_count(|(_, c_count)| *c_count)
        .unkey()
        .collect_vec();
    

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v2.1.cmp(&v1.1) // custdist desc
            .then_with(|| v2.0.cmp(&v1.0)) // c_count desc
    });

    let mut wtr = Writer::from_path(format!("{}/query13.csv", output_path)).unwrap();
    wtr.write_record(&[
        "c_count",
        "custdist",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
        
}


// query 14
fn query14(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum : f64,
        promo_revenue : f64
    }
    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();

    let res = lineitem
        .filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(param0.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(30);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            ship_date >= start_date && ship_date < end_date
        })
        .join(part, |l_row| l_row.l_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .fold_assoc(Aggregates::default(), |acc, (_, (l_row, p_row))| {
            if p_row.p_type.contains("PROMO") {
                acc.sum += l_row.l_extendedprice * (1.0 - l_row.l_discount);
            }
            acc.promo_revenue += l_row.l_extendedprice * (1.0 - l_row.l_discount);
            
        }, |acc, other| {
            acc.promo_revenue += other.promo_revenue;
            acc.sum += other.sum;
        })
        .map(|acc| acc.sum * 100.00 / acc.promo_revenue)
        .collect_vec();

    ctx.execute_blocking();

    let res = res.get().unwrap();

    let mut wtr = Writer::from_path(format!("{}/query14.csv", output_path)).unwrap();
    wtr.write_record(&[
        "promo_revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
        
    
}


// query 15
fn query15(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let lineitem = ctx.stream(lineitem_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    
    let revenue0 = lineitem
        .filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(&param0.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chronoutil::RelativeDuration::months(3);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            ship_date >= start_date && ship_date < end_date
        })
        .group_by_sum(|l_row| l_row.l_suppkey, |l_row| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .unkey()
        .collect_vec();

    ctx.execute_blocking();   
 
    let mut max_revenue = revenue0
        .get()
        .unwrap();
    max_revenue
        .sort_unstable_by(|v1,v2| 
            OrderedFloat(v2.1).cmp(&OrderedFloat(v1.1))
        );

    let max = max_revenue
        .get(0)
        .cloned()
        .unwrap()
        .1;

    
    
    let ctx2 = StreamContext::new(config.clone());
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    
    let lineitem = ctx2.stream(lineitem_source);
    let supplier = ctx2.stream(supplier_source);

    let param01 = params[0].parse::<String>().unwrap().clone();
    
    let revenue0 = lineitem
        .filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(param01.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chronoutil::RelativeDuration::months(3);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            ship_date  >= start_date && ship_date < end_date
        })
        .group_by_sum(|l_row| l_row.l_suppkey, |l_row| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .unkey();

    let res = supplier
        .join(revenue0, |s_row| s_row.s_suppkey, |(supplier_no, _total_revenue)| *supplier_no)
        .drop_key()
        .filter(move |(_, (_, total_revenue))| *total_revenue == max)
        .map(|(s_row, (_, total_revenue))| (s_row.s_suppkey, s_row.s_name, s_row.s_address, s_row.s_phone, total_revenue))
        .collect_vec();

    ctx2.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // s_suppkey
    });

    let mut wtr = Writer::from_path(format!("{}/query15.csv", output_path)).unwrap();
    wtr.write_record(&[
        "s_suppkey",
        "s_name",
        "s_address",
        "s_phone",
        "total_revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
}


// query 16
fn query16(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let partsupp = ctx.stream(partsupp_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();

    let vec0 = params[2].parse::<i32>().unwrap().clone();
    let vec1 = params[3].parse::<i32>().unwrap().clone();
    let vec2 = params[4].parse::<i32>().unwrap().clone();
    let vec3 = params[5].parse::<i32>().unwrap().clone();
    let vec4 = params[6].parse::<i32>().unwrap().clone();
    let vec5 = params[7].parse::<i32>().unwrap().clone();
    let vec6 = params[8].parse::<i32>().unwrap().clone();
    let vec7 = params[9].parse::<i32>().unwrap().clone();

    let excluded_ps_suppkey = supplier
        .filter(|s_row| s_row.s_comment.contains("Customer") || s_row.s_comment.contains("Complaints"))
        .map(|s_row| s_row.s_suppkey);

    let size_vec = vec![vec0, vec1, vec2, vec3, vec4, vec5, vec6, vec7];
    
    let res = partsupp
        .join(part.filter(move |p_row| {
            p_row.p_brand != param0.as_str() &&
            !p_row.p_type.contains(param1.as_str()) &&
            size_vec.clone().contains(&p_row.p_size)
            }), 
            |ps_row| ps_row.ps_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .left_join(excluded_ps_suppkey, |(_, (ps_row, _))| ps_row.ps_suppkey, |ex_ps| *ex_ps)
        .unkey()
        .filter(|(_, ((_, (_, _)), option_ps))| option_ps.is_none())
        .group_by_count(|(_, ((_, (_, p_row)), _))| (p_row.p_brand.clone(), p_row.p_type.clone(), p_row.p_size))
        .unkey()
        .map(|((p_brand, p_type, p_size), count)| (p_brand, p_type, p_size, count))
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v2.3.cmp(&v1.3) // supplier_cnt
            .then_with(|| v1.0.cmp(&v2.0)) // p_brand
            .then_with(|| v1.1.cmp(&v2.1)) // p_type
            .then_with(|| v1.2.cmp(&v2.2)) // p_size
    });

    let mut wtr = Writer::from_path(format!("{}/query16.csv", output_path)).unwrap();
    wtr.write_record(&[
        "p_brand",
        "p_type",
        "p_size",
        "supplier_cnt",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  

} 


// query 17
fn query17(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);
    let mut lineitem = ctx.stream(lineitem_source).split(2);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();

    let thresholds = lineitem.pop().unwrap()
        .group_by_avg(|l_row| l_row.l_partkey, |l_row| l_row.l_quantity)
        .unkey()
        .map(|(l_partkey, avg)| (l_partkey, 0.2 * avg));

        
    let res = lineitem.pop().unwrap()
        .join(part
            .filter(move |p_row| {
                p_row.p_brand == param0.as_str() &&
                p_row.p_container == param1.as_str()
            }), 
            |l_row| l_row.l_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .join(thresholds, |(_, (l_row, _))| l_row.l_partkey, |(l_partkey, _)| *l_partkey)
        .unkey()
        .filter(|(_, ((_, (l_row, _)), (_, avg)))| l_row.l_quantity < *avg)
        .map(|(_, ((_, (l_row, _)), (_, _)))| l_row.l_extendedprice)
        .fold_assoc(0.0, |sum, l_extendedprice| *sum += l_extendedprice, |sum, other| *sum += other)
        .map(|sum| sum / 7.0)
        .collect_vec();

    ctx.execute_blocking();

    let res = res.get().unwrap();

    let mut wtr = Writer::from_path(format!("{}/query17.csv", output_path)).unwrap();
    wtr.write_record(&[
        "avg_yearly",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    

}


// query 18  
fn query18(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let mut lineitem = ctx.stream(lineitem_source).split(2);

    let param0 = params[0].parse::<f64>().unwrap().clone();

    let subset = lineitem.pop().unwrap()
        .group_by_sum(|l_row| l_row.l_orderkey, |l_row| l_row.l_quantity)
        .unkey()
        .filter(move |(_, sum)| *sum > param0);

    let res = customer
        .join(orders, |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        .join(lineitem.pop().unwrap(), |(_, (_, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .join(subset, |(_, ((_, (_, o_row)), _))| o_row.o_orderkey, |(l_orderkey, _)| *l_orderkey)
        .unkey()
        .group_by_sum(|(_, ((_, ((_, (c_row, o_row)), _)), (_, _)))| (c_row.c_name.clone(), c_row.c_custkey, o_row.o_orderkey, o_row.o_orderdate.clone(), o_row.o_totalprice as u64)
                    ,|(_, ((_, ((_, (_, _)), l_row)), (_, _)))| l_row.l_quantity)
        .unkey()
        .map(|((c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice), l_quantity)| (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity))
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v2.4.cmp(&v1.4) // o_totalprice
            .then_with(|| {
                let v1_orderdate = NaiveDate::parse_from_str(v1.3.as_str(), "%Y-%m-%d").unwrap();
                let v2_orderdate = NaiveDate::parse_from_str(v2.3.as_str(), "%Y-%m-%d").unwrap();

                v1_orderdate.cmp(&v2_orderdate)
            })
    });

    let mut wtr = Writer::from_path(format!("{}/query18.csv", output_path)).unwrap();
    wtr.write_record(&[
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
        "sum(l_quantity)",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
}

// query 19 
fn query19(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    
    let part = ctx.stream(part_source);
    let lineitem = ctx.stream(lineitem_source);    

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    let param2 = params[2].parse::<String>().unwrap().clone();

    let param3 = params[3].parse::<f64>().unwrap().clone();
    let param4 = params[4].parse::<f64>().unwrap().clone();
    let param5 = params[5].parse::<f64>().unwrap().clone();
   
    let res = lineitem
        .join(part, |l_row| l_row.l_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .filter(move |(_, (l_row, p_row))| {
            let container_sm = vec!["SM CASE", "SM BOX", "SM PACK", "SM PKG"];
            let container_med = vec!["MED BAG", "MED BOX", "MED PACK", "MED PKG"];
            let container_lg = vec!["LG CASE", "LG BOX", "LG PACK", "LG PKG"];
            let shipmode = vec!["AIR", "AIR REG"];
            (
                p_row.p_brand == param0.as_str() &&
                container_sm.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= param3 && l_row.l_quantity <= 1.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=5 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            ) ||
            (
                p_row.p_brand == param1.as_str() &&
                container_med.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= param4 && l_row.l_quantity <= 10.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=10 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            ) ||
            (
                p_row.p_brand == param2.as_str() &&
                container_lg.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= param5 && l_row.l_quantity <= 20.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=15 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            )
        })
        .fold_assoc(0.0, |sum, (_, (l_row, _))| *sum += l_row.l_extendedprice * (1.0 - l_row.l_discount), |sum, other| *sum += other)
        .collect_vec();

    ctx.execute_blocking();

    let res = res.get().unwrap();
    
    let mut wtr = Writer::from_path(format!("{}/query19.csv", output_path)).unwrap();
    wtr.write_record(&[
        "revenue",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
} 


// query 20
fn query20(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path)).has_headers(false);
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);

    let part = ctx.stream(part_source);  
    let supplier = ctx.stream(supplier_source);
    let partsupp = ctx.stream(partsupp_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);

    let param0 = params[0].parse::<String>().unwrap().clone();
    let param1 = params[1].parse::<String>().unwrap().clone();
    let param2 = params[2].parse::<String>().unwrap().clone();


    let availqty_threshold = lineitem
        .filter(move |l_row| {
            let start_date = NaiveDate::parse_from_str(param1.as_str(), "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(365);
            let l_shipdate = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            l_shipdate >= start_date && l_shipdate < end_date
        })
        .group_by_sum(|l_row| (l_row.l_partkey, l_row.l_suppkey), |l_row| l_row.l_quantity) 
        .unkey()
        .map(|((l_partkey, l_suppkey), sum)| (l_partkey, l_suppkey, 0.5 * sum));
    
    let suppkeys = partsupp
        .join(part.filter(move |p_row| p_row.p_name.starts_with(param0.as_str())), 
            |ps_row| ps_row.ps_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .join(availqty_threshold, |(_, (ps_row, _))| (ps_row.ps_partkey, ps_row.ps_suppkey), |(l_partkey, l_suppkey, _)| (*l_partkey, *l_suppkey))
        .unkey()
        .filter(|((_, _), ((_, (ps_row, _)), (_, _, sum)))| ps_row.ps_availqty as f64 > *sum)
        .map(|((_, _), ((_, (ps_row, _)), (_, _, _)))| ps_row.ps_suppkey);
 
    let res = supplier
        .join(nation.filter(move |n_row| n_row.n_name == param2.as_str()), |s_row| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(suppkeys, |(_, (s_row, _))| s_row.s_suppkey, |suppkey| *suppkey)
        .unkey()
        .map(|(_, ((_, (s_row, _)), _))| (s_row.s_name, s_row.s_address))
        .rich_filter_map({
            let mut seen_keys = std::collections::HashSet::new();
            move |(s_name, s_address)| {
                if seen_keys.insert(s_name.clone()) {
                    Some((s_name.clone(), s_address))
                } else {
                    None
                }
            }
        })
        
        .collect_vec();

    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // s_name
    });

    let mut wtr = Writer::from_path(format!("{}/query20.csv", output_path)).unwrap();
    wtr.write_record(&[
        "s_name",
        "s_address",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
} 

// query 21
fn query21(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {
    
    let ctx = StreamContext::new(config.clone());

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path)).has_headers(false);
    
    
    let mut lineitem = ctx.stream(lineitem_source).split(3);    
    let orders = ctx.stream(orders_source);
    let supplier = ctx.stream(supplier_source);
    let nation = ctx.stream(nation_source);

    let param0 = params[0].parse::<String>().unwrap().clone();

    // not exist part // all_l1
    let mut all_l1 = supplier
        .join(lineitem.pop().unwrap().filter(|l_row| {
            let l_receiptdate = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            let l_commitdate = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();

            l_receiptdate > l_commitdate
        }), |s_row| s_row.s_suppkey, |l_row| l_row.l_suppkey)
        .drop_key()
        
        .join(orders.filter(|o_row| o_row.o_orderstatus == "F"), |(_, l_row)| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .drop_key()
        .join(nation.filter(move |n_row| n_row.n_name == param0.as_str()), |((s_row, _), _)| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .drop_key()
        .split(3);

    // l3
    let not_exist = lineitem.pop().unwrap()
        .join(all_l1.pop().unwrap(), |l_row| l_row.l_orderkey, |(((_, l_row),_),_)| l_row.l_orderkey)
        .drop_key()
        .filter(|(l3_row, (((_, l_row), _), _))| {
            let l_receiptdate = NaiveDate::parse_from_str(&l3_row.l_receiptdate, "%Y-%m-%d").unwrap();
            let l_commitdate = NaiveDate::parse_from_str(&l3_row.l_commitdate, "%Y-%m-%d").unwrap();

            l_receiptdate > l_commitdate && l_row.l_suppkey != l3_row.l_suppkey
        })
        .map(|(l3_row, (((_, _), _), _))| l3_row.l_orderkey) 
        .unique_assoc();

    let exist = lineitem.pop().unwrap()
        .join(all_l1.pop().unwrap(), |l_row| l_row.l_orderkey, |(((_, l_row),_),_)| l_row.l_orderkey)
        .drop_key()
        .filter(|(l2_row, (((_, l_row), _), _))| l2_row.l_suppkey != l_row.l_suppkey )
        .map(|(l2_row, (((_, _), _), _))| l2_row.l_orderkey)
        .unique_assoc();

    
    
    let res = all_l1.pop().unwrap()
        .left_join(not_exist, |(((_, l_row), _), _)| l_row.l_orderkey, |l_row| *l_row)
        .drop_key()
        .filter(|((((_, _), _), _),l_row)| l_row.is_none())
        .join(exist, |((((_, l1_row), _), _),_)| l1_row.l_orderkey, |l2_row| *l2_row)
        .drop_key()
        .map(|(((((s_row, _), _), _),_),_)| s_row.s_name)
        .group_by_count(|s_name| s_name.clone())
        .unkey()
        .collect_vec();
    
    ctx.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v2.1.cmp(&v1.1) // numwait
           .then_with(|| v1.0.cmp(&v2.0)) // s_name
    });

    let mut wtr = Writer::from_path(format!("{}/query21.csv", output_path)).unwrap();
    wtr.write_record(&[
        "s_name",
        "numwait",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
} 


// query 22
fn query22(config:RuntimeConfig, input_path:&str, output_path:&str, params: Vec<String>) {

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path)).has_headers(false);    
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path)).has_headers(false);
    

    // first part to calcultate the average value
    let ctx = StreamContext::new(config.clone());
    let customer = ctx.stream(customer_source.clone());

    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum : f64,
        count: i32
    }

    let pre0 = params[0].parse::<String>().unwrap().clone();
    let pre1 = params[1].parse::<String>().unwrap().clone();
    let pre2 = params[2].parse::<String>().unwrap().clone();
    let pre3 = params[3].parse::<String>().unwrap().clone();
    let pre4 = params[4].parse::<String>().unwrap().clone();
    let pre5 = params[5].parse::<String>().unwrap().clone();
    let pre6 = params[6].parse::<String>().unwrap().clone();

    let c_acctbal_avg = customer
        .filter(move |c_row| {
            let prefixes = vec![pre0.as_str(), pre1.as_str(), pre2.as_str(), pre3.as_str(), pre4.as_str(), pre5.as_str(), pre6.as_str()];
            c_row.c_acctbal > 0.00 &&
            prefixes.contains(&(&(c_row.c_phone.to_string())[0..2]))
        })
        .fold_assoc(Aggregates::default(), 
            |acc, c_row| {
                acc.sum += c_row.c_acctbal;
                acc.count += 1;
            }, |acc, other| {
                acc.sum += other.sum;
                acc.count += other.count;
            })
        .map(|acc| acc.sum/acc.count as f64)
        .collect::<Vec<f64>>();
    
    ctx.execute_blocking();

    let c_acctbal_avg_value = c_acctbal_avg
            .get()
            .unwrap()
            .get(0)
            .cloned()
            .unwrap();

    // second part, the actual query
    let ctx2 = StreamContext::new(config.clone());
   
    let orders = ctx2.stream(orders_source.clone());
    let customer = ctx2.stream(customer_source.clone());

    let pre01 = params[0].parse::<String>().unwrap().clone();
    let pre11 = params[1].parse::<String>().unwrap().clone();
    let pre21 = params[2].parse::<String>().unwrap().clone();
    let pre31 = params[3].parse::<String>().unwrap().clone();
    let pre41 = params[4].parse::<String>().unwrap().clone();
    let pre51 = params[5].parse::<String>().unwrap().clone();
    let pre61 = params[6].parse::<String>().unwrap().clone();

    let custsale = customer
        .filter(move |c_row| {
            let prefixes = vec![pre01.as_str(), pre11.as_str(), pre21.as_str(), pre31.as_str(), pre41.as_str(), pre51.as_str(), pre61.as_str()];

            prefixes.contains(&(&(c_row.c_phone.to_string())[0..2])) &&
            c_row.c_acctbal > c_acctbal_avg_value
        })
        .left_join(orders, |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .filter(|(_, (_, option_row))| option_row.is_none())
        .unkey()
        .map(|(_, (c_row, _))| ((&c_row.c_phone[0..2]).to_string(), c_row.c_acctbal));

    
    let res = custsale
        .group_by_fold(|(cntrycode, _)| cntrycode.clone(), Aggregates::default(), 
            |acc, (_, c_acctbal)| {
                acc.sum += c_acctbal;
                acc.count += 1;
            },
            |acc, other| {
                acc.sum += other.sum;
                acc.count += other.count;
            }
        )
        .unkey()
        .map(|(cntrycode, acc)| (cntrycode.to_string(), acc.count, acc.sum))
        .collect_vec();
    
    ctx2.execute_blocking();

    let mut res = res.get().unwrap();
    res.sort_unstable_by(|v1, v2|  {
        v1.0.cmp(&v2.0) // cntrycode
    });

    let mut wtr = Writer::from_path(format!("{}/query22.csv", output_path)).unwrap();
    wtr.write_record(&[
        "cntrycode",
        "numcust",
        "totacctbal",
    ])
    .unwrap();

    for record in res {
        wtr.serialize(record).unwrap();
    }  
    
}


const BENCHMARK: &str = "TPC-H";
const BACKEND: &str = "renoir";
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

fn main() {
    let input_path = env::var("INPUT_PATH").expect("INPUT_PATH is not set");
    let input_path = format!("{}/", input_path);
    let config =  RuntimeConfig::default();

    let output_path = env::var("OUTPUT_PATH").expect("OUTPUT_PATH is not set");
    let output_path = format!("{}/renoir", output_path);
    if let Err(_) = create_dir_all(&output_path) {
    } else {
        println!("Output directory is ready at {:?}", output_path);
    }
    
    let queries = vec![
        query1, 
        query2, 
        query3, 
        query4, 
        query5, 
        query6, 
        query7,
        query8, 
        query9, 
        query10, 
        query11, 
        query12, 
        query13, 
        query14, 
        query15, 
        query16, 
        query17, 
        query18, 
        query19, 
        query20, 
        query21, 
        query22
    ];
    

    let params_file = format!("{}/queries_parameters.json", input_path);
    let params: serde_json::Value = {
        let file = std::fs::File::open(params_file).expect("Unable to open params file");
        serde_json::from_reader(file).expect("Unable to parse params file")
    };

    let mut query_number = 1;
    for query in queries {
        let start = std::time::Instant::now();        
        let param_values = params[query_number.to_string()]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        query(config.clone(), input_path.as_str(), output_path.as_str(), param_values);
        
            
        let duration = start.elapsed();
        println!("Query {} -> execution_time: {:?}", query_number, duration.as_secs_f64());
        if let Err(e) = send_execution_time(query_number, 1, duration.as_secs_f64()) {
            eprintln!("Error: {}", e);
        }
        
        query_number+=1;
        }
}


/*
    let ctx = StreamContext::new_local();
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", INPUT_PATH)).has_headers(false);
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", INPUT_PATH)).has_headers(false);
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", INPUT_PATH)).has_headers(false);
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", INPUT_PATH)).has_headers(false);
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", INPUT_PATH)).has_headers(false);    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", INPUT_PATH)).has_headers(false);
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", INPUT_PATH)).has_headers(false);
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", INPUT_PATH)).has_headers(false);

    let part = ctx.stream(part_source);  
    let supplier = ctx.stream(supplier_source);
    let partsupp = ctx.stream(partsupp_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let region = ctx.stream(region_source);

    // for debuggin purpose when the type of data is not automatically inferred
    //let _: () = data; 
    
    let _query = nation
        .join(region, |n| n.n_regionkey, |r| r.r_regionkey)
        .drop_key()
        .write_csv_seq("./src/data/job1/query1.csv".into(), true);
    ctx.execute_blocking();
 */