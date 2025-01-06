
use std::error::Error;
use std::env;
use serde_json::json;
use renoir::prelude::*;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use chrono::{Datelike, NaiveDate};
use ordered_float::OrderedFloat;


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
fn query1(config:RuntimeConfig, input_path:&str) -> StreamContext{

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
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let lineitem = ctx.stream(lineitem_source);


    lineitem
        .filter(|row| {
            // Parse the `l_shipdate` string to a `NaiveDate` for comparison
            let ship_date = NaiveDate::parse_from_str(&row.l_shipdate, "%Y-%m-%d").unwrap();
            let filter_date = NaiveDate::parse_from_str("1998-12-01", "%Y-%m-%d")
                .unwrap()
                - chrono::Duration::days(90);
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
    })
    .collect_vec();

    ctx  
}

// query 2
fn query2(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path));

    let part = ctx.stream(part_source);
    let mut supplier = ctx.stream(supplier_source).split(2);
    let mut partsupp = ctx.stream(partsupp_source).split(2);
    let mut nation = ctx.stream(nation_source).split(2);
    let mut region = ctx.stream(region_source).split(2);

        // Find the minimum ps_supplycost for each p_partkey in the "EUROPE" region
    let min_supply_cost = partsupp.pop().unwrap()
        .join(supplier.pop().unwrap(), |ps_row| ps_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation.pop().unwrap(), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(region.pop().unwrap().filter(|r_row| r_row.r_name == "EUROPE"), |(_, ((_,(_, _)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey)
        .unkey()
        .map(|row: (i32, ((i32, ((i32, (PartsuppRow, SupplierRow)), NationRow)), RegionRow))| (row.1.0.1.0.1.0.ps_partkey, row.1.0.1.0.1.0.ps_supplycost))
        .group_by_min_element(                                              
           |(ps_key, _)| ps_key.to_owned(),           
           |(_, ps_cost)| OrderedFloat(ps_cost.to_owned()),
        )
        .drop_key();
        //.inspect(|row| println!("Item: {:?}", row)).for_each(std::mem::drop);  // (i32 ps_partkey, f64 mis_supply_cost) 
    

    part
        .filter(|p_row| 
            (p_row.p_size == 15) && (p_row.p_type.contains("BRASS"))
        )
        .join(partsupp.pop().unwrap(), |p_row| p_row.p_partkey, |ps_row| ps_row.ps_partkey)
        .unkey()
        .join(supplier.pop().unwrap(), |(_, (_, ps_row))| ps_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation.pop().unwrap(), |(_, ((_, (_, _)), s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(region.pop().unwrap().filter(|r_row| r_row.r_name == "EUROPE"), |(_, ((_, ((_, (_, _)), _)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey )
        .unkey()
        .join(min_supply_cost, |(_, ((_, ((_, ((_, (_, ps_row)), _)), _)), _))| (ps_row.ps_partkey, OrderedFloat(ps_row.ps_supplycost)), |min_ps| (min_ps.0, OrderedFloat(min_ps.1)))
        .unkey()
      //let _: () = inter;  
        .map(|((_, _ps_min_supp_cost), ((_r_key, ((_s_n_key, ((_ps_suppkey, ((_ps_partkey, (p_row, _ps_row)), s_row)), n_row)), _r_row)), (_, _)))|  (s_row.s_acctbal, s_row.s_name, n_row.n_name, p_row.p_partkey, p_row.p_mfgr, s_row.s_address, s_row.s_phone, s_row.s_comment))
        .collect_vec();
        ctx
    // todo order by

    

}


// query 3
fn query3(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));


    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);

    customer
        .filter(|c_row| c_row.c_mktsegment == "BUILDING")
        .join(orders.filter(|o_row| {
            let o_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let filter_date = NaiveDate::parse_from_str("1995-03-15", "%Y-%m-%d").unwrap();
            o_date < filter_date}), 
            |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        //.map(|row: (i32, (CustomerRow, OrdersRow))| row.1)

        .join(lineitem.filter(|l_row| {
            let l_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let filter_date = NaiveDate::parse_from_str("1995-03-15", "%Y-%m-%d").unwrap();
            l_date > filter_date}) 
            , |(_,(_, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        //.map(|row: (i32, ((CustomerRow, OrdersRow), LineitemRow))| row.1)
        .group_by_sum(|(_, ((_, (_, o_row)), l_row))| (l_row.l_orderkey, o_row.o_orderdate.clone(), o_row.o_shippriority), 
                        |(_, ((_, (_, _)), l_row))| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        // todo.map()
        .unkey()
        .map(|row| (row.0.0, row.0.1, row.0.2, row.1))
        .collect_vec();
        // todo non ho capito perchè non funziona questa seconda parte    
    ctx
}

// query 4
fn query4(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));

    let mut orders = ctx.stream(orders_source).split(2);
    let lineitem = ctx.stream(lineitem_source);

    let exist_query = lineitem
        .filter(|l_row| {
            let commit_date = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();
            let receipt_date = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            commit_date < receipt_date
        })
        .join(orders.pop().unwrap(), |l_row| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .map(|(_, (_, o_row))| o_row.o_orderkey);
    
    orders.pop().unwrap()
        .filter(|o_row| {
            let start_date = NaiveDate::parse_from_str("1993-07-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(90);
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            (order_date >= start_date) && (order_date < end_date)
        })
        .join(exist_query, |o_row| o_row.o_orderkey, |order_key| *order_key)
        .unkey()
        .group_by_count(|(_, (o_row, _))| o_row.o_orderpriority.to_string())
        .collect_vec();


    ctx
}


// query 5
fn query5(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);

    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path));


    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let region = ctx.stream(region_source);

    
    customer
        .join(orders
            .filter(|o_row| {
                let start_date = NaiveDate::parse_from_str("1994-01-01", "%Y-%m-%d").unwrap();
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
        .join(region.filter(|r_row| r_row.r_name.to_string().eq("ASIA")), |(_, ((_, ((_, ((_, (_c_row, _o_row)), _l_row)), _s_row)), n_row))| n_row.n_regionkey, |r_row| r_row.r_regionkey)
        .unkey()
        .map(|(_, ((_, ((_, ((_, ((_, (_c_row, _o_row)), l_row)), _s_row)), n_row)), _r_row))| (n_row.n_name, l_row.l_extendedprice, l_row.l_discount))
        .group_by_sum(|(n_name, _, _)| n_name.to_string(), |(_, l_extendeprice, l_discount)| l_extendeprice * (1.0 - l_discount))
        .unkey()
        .collect_vec();
    
    ctx
}

// query 6
fn query6(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let lineitem = ctx.stream(lineitem_source);

    
    lineitem
        .filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1994-01-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(365);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            (ship_date >= start_date) && (ship_date < end_date) 
            && (l_row.l_discount >= 0.06-0.01) && (l_row.l_discount <= 0.06+0.01)
            && (l_row.l_quantity < 24.0)
        })
        .fold_assoc(0.0, |sum, l_row| *sum += l_row.l_extendedprice * l_row.l_discount, |sum1, sum2| *sum1 += sum2)
        .collect_vec();
    
    
    ctx
}

// query 7
fn query7(config:RuntimeConfig, input_path:&str) -> StreamContext{


    let ctx = StreamContext::new(config);

    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));


    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let mut nation = ctx.stream(nation_source).split(2);
    

    let nation1 = nation.pop().unwrap();
    let nation2 = nation.pop().unwrap();

    
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
        .filter(|(_, ((_, ((_, ((_, ((_, (_, l_row)), _)), _)), n1_row)), n2_row))| {
            let start_date = NaiveDate::parse_from_str("1995-01-01", "%Y-%m-%d").unwrap();
            let end_date = NaiveDate::parse_from_str("1996-12-31", "%Y-%m-%d").unwrap();
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            
            (n1_row.n_name == "FRANCE") && (n2_row.n_name == "GERMANY")
            || (n1_row.n_name == "GERMANY") && (n2_row.n_name == "FRANCE")
            && (ship_date >= start_date && ship_date <= end_date)
        })
        .unkey()
        .map(|(_, ((_, ((_, ((_, ((_, (_, l_row)), _)), _)), n1_row)), n2_row))| {
            let l_shipdate = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let l_year = l_shipdate.year().to_string();
            (n1_row.n_name, n2_row.n_name, l_year, l_row.l_extendedprice * (1.0 - l_row.l_discount))
        });
    
    shipping
        .group_by_sum(|(supp_nation, cust_nation, l_year, _)| (supp_nation.clone(), cust_nation.clone(), l_year.clone()), |(_, _, _, volume)| volume)
        .unkey()
        .collect_vec();

    ctx
}


// query 8
fn query8(config:RuntimeConfig, input_path:&str) -> StreamContext{
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum_brazil : f64,
        sum_tot: f64
    }

    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", input_path));

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let mut nation = ctx.stream(nation_source).split(2);
    let region = ctx.stream(region_source);

    let nation1 = nation.pop().unwrap();
    let nation2 = nation.pop().unwrap();

    let all_nations = part
        .filter(|p_row| p_row.p_type == "ECONOMY ANODIZED STEEL")
        .join(lineitem, |p_row| p_row.p_partkey, |l_row| l_row.l_partkey)
        .unkey()
        .join(supplier, |(_, (_, l_row))| l_row.l_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(orders.filter(|o_row| {
            let start_date = NaiveDate::parse_from_str("1995-01-01", "%Y-%m-%d").unwrap();
            let end_date = NaiveDate::parse_from_str("1996-12-31", "%Y-%m-%d").unwrap();
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();

            order_date >= start_date && order_date <= end_date
        }), |(_, ((_, (_, l_row)), _))| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .join(customer, |(_, ((_, ((_, (_, _)), _)), o_row))| o_row.o_custkey, |c_row| c_row.c_custkey)
        .unkey()
        .join(nation1, |(_, ((_, ((_, ((_, (_, _)), _)), _)), c_row))| c_row.c_nationkey, |n1_row| n1_row.n_nationkey)
        .unkey()
        .join(nation2, |(_, ((_, ((_, ((_, ((_, (_, _)), s_row)), _)), _)), _))| s_row.s_nationkey, |n2_row| n2_row.n_nationkey)
        .unkey()
        .join(region.filter(|r_row| r_row.r_name == "AMERICA"), |(_, ((_, ((_, ((_, ((_, ((_, (_, _)), _)), _)), _)), n1_row)), _))| n1_row.n_regionkey, |r_row| r_row.r_regionkey)
        .unkey()
        .map(|(_, ((_, ((_, ((_, ((_, ((_, ((_, (_, l_row)), _)), o_row)), _)), _)), n2_row)), _))| {
            let o_orderdate = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let o_year = o_orderdate.year().to_string();

            (o_year, l_row.l_extendedprice * (1.0 - l_row.l_discount), n2_row.n_name)
        });
        
    
    all_nations
        .group_by_fold(|(o_year, _, _)| o_year.clone(), Aggregates::default(), |acc, (_, volume, nation)| {
            if nation == "BRAZIL" {
                acc.sum_brazil += volume;
            }
            acc.sum_tot += volume;
        },
            |acc, other| {
                acc.sum_brazil += other.sum_brazil;
                acc.sum_tot += other.sum_tot;
            }
        )
        .unkey()
        .map(|(o_year, acc)| (o_year.clone(), acc.sum_brazil/acc.sum_tot))
        .collect_vec();
        

    ctx
    
}


// query 9
fn query9(config:RuntimeConfig, input_path:&str) -> StreamContext{

    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path));

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let partsupp = ctx.stream(partsupp_source);

    let profit = supplier
        .join(lineitem, |s_row| (s_row.s_suppkey), |l_row| l_row.l_suppkey)
        .unkey()
        .join(partsupp, |(_, (_, l_row))| (l_row.l_suppkey, l_row.l_partkey), |ps_row|(ps_row.ps_suppkey, ps_row.ps_partkey))
        .unkey()
        .join(part.filter(|p_row| p_row.p_name.contains("green")), |((_, _), ((_, (_, l_row)), _))| l_row.l_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .join(orders, |(_, (((_, _), ((_, (_, l_row)), _)), _))| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .join(nation, |(_, ((_, (((_, _), ((_, (s_row, _)), _)), _)), _))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .map(|(_, ((_, ((_, (((_, _), ((_, (_, l_row)), ps_row)), _)), o_row)), n_row))| {
            let o_orderdate = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();
            let o_year = o_orderdate.year().to_string();
            (n_row.n_name, o_year, l_row.l_extendedprice * (1.0 - l_row.l_discount) - ps_row.ps_supplycost * l_row.l_quantity)
        });

    profit
        .group_by_sum(|(nation, o_year, _)| (nation.clone(), o_year.clone()), |(_, _, amount)| amount)
        .collect_vec();

    ctx
    
}


// query 10
fn query10(config:RuntimeConfig, input_path:&str) -> StreamContext{

    let ctx = StreamContext::new(config);

    
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));

    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);
    let customer = ctx.stream(customer_source);

    customer
        .join(orders.filter(|o_row| {
            let start_date = NaiveDate::parse_from_str("1993-10-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(90);
            let order_date = NaiveDate::parse_from_str(&o_row.o_orderdate, "%Y-%m-%d").unwrap();

            order_date >= start_date && order_date < end_date
        }), |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        .join(lineitem.filter(|l_row| l_row.l_returnflag == "R"), |(_, (_, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .join(nation, |(_, ((_, (c_row, _)), _))| c_row.c_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .group_by_sum(|(_, ((_, ((_, (c_row, _)), _)), n_row))| (c_row.c_custkey, c_row.c_name.clone(), c_row.c_acctbal as u64, n_row.n_name.clone(), c_row.c_address.clone(), c_row.c_phone.clone(), c_row.c_comment.clone()), 
            |(_, ((_, ((_, (_, _)), l_row)), _))| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .collect_vec();


    ctx
    
}


// query 11
fn query11(config:RuntimeConfig, input_path:&str) -> StreamContext{

    let ctx = StreamContext::new(config.clone());

    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    
    let partsupp = ctx.stream(partsupp_source.clone());
    let supplier = ctx.stream(supplier_source.clone());
    let nation = ctx.stream(nation_source.clone());
    

    let threshold = 
        partsupp
            .join(supplier, |p_row| p_row.ps_suppkey, |s_row| s_row.s_suppkey)
            .unkey()
            .join(nation.filter(|n_row| n_row.n_name == "GERMANY"), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
            .unkey()
            .fold_assoc(0.0, |sum, (_, ((_, (ps_row, _)), _))| *sum += ps_row.ps_supplycost * ps_row.ps_availqty as f64, |sum, other| *sum += other)
            .map(|sum| sum * 0.0001)
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
    
    partsupp2
        .join(supplier2, |p_row| p_row.ps_suppkey, |s_row| s_row.s_suppkey)
        .unkey()
        .join(nation2.filter(|n_row| n_row.n_name == "GERMANY"), |(_, (_, s_row))| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .group_by_sum(
            |(_, ((_, (ps_row, _)), _))| ps_row.ps_partkey, // Grouping by part key
            |(_, ((_, (ps_row, _)), _))| ps_row.ps_supplycost * ps_row.ps_availqty as f64,
        )
        .unkey()
        .filter(move |(_, value)| *value > threshold_value) 
        .collect_vec();
    
    ctx2
}


// query 12
fn query12(config:RuntimeConfig, input_path:&str) -> StreamContext{
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        high_line_count : i32,
        low_line_count : i32
    }

    let ctx = StreamContext::new(config);

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));

    let orders = ctx.stream(orders_source);
    let lineitem = ctx.stream(lineitem_source);

    orders
        .join(lineitem.filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1994-01-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(365);
            let receipt_date = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();
            let commit_date = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();
            
            (l_row.l_shipmode == "MAIL" ||  l_row.l_shipmode == "SHIP") 
            && (receipt_date >= start_date && receipt_date <= end_date)
            && (commit_date < receipt_date && ship_date < commit_date)
        }), |o_row| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .group_by_fold(|(_, (_, l_row))| l_row.l_shipmode.clone(), Aggregates::default(), 
            |acc, (_, (o_row, _))| {
                if o_row.o_orderpriority == "1-URGENT" && o_row.o_orderpriority == "2-HIGH" {
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

        //todo non torna il risultato
    ctx
    
}

// query 13
fn query13(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config);

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    
    let orders = ctx.stream(orders_source);
    let customer = ctx.stream(customer_source);

    let c_orders = customer
        .left_join(orders.filter(|o_row| !o_row.o_comment.contains("special") && !o_row.o_comment.contains("requests")),|c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        .group_by_count(|(_, (c_row, _))| c_row.c_custkey)
        .unkey();

    c_orders
        .group_by_count(|(_, c_count)| *c_count)
        .collect_vec();
    


        //todo non torna il risultato()
    ctx
    
}


// query 14
fn query14(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum : f64,
        promo_revenue : f64
    }
    let ctx = StreamContext::new(config);

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));

    let part = ctx.stream(part_source);
    let lineitem = ctx.stream(lineitem_source);

    lineitem
        .filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1995-09-01", "%Y-%m-%d").unwrap();
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
        //todo non torna il risultato
    ctx
    
}


// query 15
fn query15(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let lineitem = ctx.stream(lineitem_source);
    
    let revenue0 = lineitem
        .filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1996-01-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(90);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            ship_date >= start_date && ship_date < end_date
        })
        .group_by_sum(|l_row| l_row.l_suppkey, |l_row| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .unkey()
        .reduce(|(supp_1, tot_rev_1), (supp_2, tot_rev_2)| {
            if tot_rev_1 >= tot_rev_2 {
                (supp_1, tot_rev_1)
            } else {
                (supp_2, tot_rev_2)
            }
        })
        .collect::<Vec<(i32,f64)>>();
    
    ctx.execute_blocking();
 
    let max_revenue = revenue0
        .get()
        .unwrap()
        .get(0)
        .cloned()
        .unwrap()
        .1;
    
    let ctx2 = StreamContext::new(config.clone());
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    
    let lineitem = ctx2.stream(lineitem_source);
    let supplier = ctx2.stream(supplier_source);
    
    let revenue0 = lineitem
        .filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1996-01-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(90);
            let ship_date = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            ship_date  >= start_date && ship_date < end_date
        })
        .group_by_sum(|l_row| l_row.l_suppkey, |l_row| l_row.l_extendedprice * (1.0 - l_row.l_discount))
        .unkey();

    supplier
        .join(revenue0, |s_row| s_row.s_suppkey, |(supplier_no, _total_revenue)| *supplier_no)
        .unkey()
        .filter(move |(_, (_, (_, total_revenue)))| *total_revenue == max_revenue)
        .map(|(_, (s_row, (_, total_revenue)))| (s_row.s_suppkey, s_row.s_name, s_row.s_address, s_row.s_phone, total_revenue))
        .collect_vec();
    
    ctx2
    
}


// query 16
fn query16(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path));

    let part = ctx.stream(part_source);
    let supplier = ctx.stream(supplier_source);
    let partsupp = ctx.stream(partsupp_source);

    let excluded_ps_suppkey = supplier
        .filter(|s_row| s_row.s_comment.contains("Customer") || s_row.s_comment.contains("Complaints"))
        .map(|s_row| s_row.s_suppkey);

    let size_vec = vec![49, 14, 23, 45, 19, 3, 36, 9];
    
    partsupp
        .join(part.filter(move |p_row| {
            p_row.p_brand != "BRAND#45" &&
            !p_row.p_type.contains("MEDIUM POLISHED") &&
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
        
    ctx
    
} 


// query 17
fn query17(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));

    let part = ctx.stream(part_source);
    let mut lineitem = ctx.stream(lineitem_source).split(2);

    let thresholds = lineitem.pop().unwrap()
        .group_by_avg(|l_row| l_row.l_partkey, |l_row| l_row.l_quantity)
        .unkey()
        .map(|(l_partkey, avg)| (l_partkey, 0.2 * avg));

        
    lineitem.pop().unwrap()
        .join(part
            .filter(|p_row| {
                p_row.p_brand == "Brand#23" &&
                p_row.p_container == "MED BOX"
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
    
        
    ctx
    
}


// query 18  
fn query18(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    
    let customer = ctx.stream(customer_source);
    let orders = ctx.stream(orders_source);
    let mut lineitem = ctx.stream(lineitem_source).split(2);

    let subset = lineitem.pop().unwrap()
        .group_by_sum(|l_row| l_row.l_orderkey, |l_row| l_row.l_quantity)
        .unkey()
        .filter(|(_, sum)| *sum > 300.0);

    customer
        .join(orders, |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .unkey()
        .join(lineitem.pop().unwrap(), |(_, (_, o_row))| o_row.o_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .join(subset, |(_, ((_, (_, o_row)), _))| o_row.o_orderkey, |(l_orderkey, _)| *l_orderkey)
        .unkey()
        .group_by_sum(|(_, ((_, ((_, (c_row, o_row)), _)), (_, _)))| (c_row.c_name.clone(), c_row.c_custkey, o_row.o_orderkey, o_row.o_orderdate.clone(), o_row.o_totalprice as u64)
                    ,|(_, ((_, ((_, (_, _)), l_row)), (_, _)))| l_row.l_quantity)
        .collect_vec();

        
    ctx
    
}

// query 19 
fn query19(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    
    let part = ctx.stream(part_source);
    let lineitem = ctx.stream(lineitem_source);    
   
    lineitem
        .join(part, |l_row| l_row.l_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .filter(|(_, (l_row, p_row))| {
            let container_sm = vec!["SM CASE", "SM BOX", "SM PACK", "SM PKG"];
            let container_med = vec!["MED BAG", "MED BOX", "MED PACK", "MED PKG"];
            let container_lg = vec!["LG CASE", "LG BOX", "LG PACK", "LG PKG"];
            let shipmode = vec!["AIR", "AIR REG"];
            (
                p_row.p_brand == "Brand#12" &&
                container_sm.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= 1.0 && l_row.l_quantity <= 1.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=5 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            ) ||
            (
                p_row.p_brand == "Brand#23" &&
                container_med.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= 10.0 && l_row.l_quantity <= 10.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=10 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            ) ||
            (
                p_row.p_brand == "Brand#34" &&
                container_lg.contains(&p_row.p_container.as_str()) &&
                l_row.l_quantity >= 20.0 && l_row.l_quantity <= 20.0+10.0 &&
                p_row.p_size >= 1 && p_row.p_size <=15 &&
                shipmode.contains(&l_row.l_shipmode.as_str()) &&
                l_row.l_shipinstruct == "DELIVER IN PERSON"
            )
        })
        .fold_assoc(0.0, |sum, (_, (l_row, _))| *sum += l_row.l_extendedprice * (1.0 - l_row.l_discount), |sum, other| *sum += other)
        .collect_vec();

    ctx
    
} 


// query 20
fn query20(config:RuntimeConfig, input_path:&str) -> StreamContext{

    let ctx = StreamContext::new(config.clone());

    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", input_path));
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));

    let part = ctx.stream(part_source);  
    let supplier = ctx.stream(supplier_source);
    let partsupp = ctx.stream(partsupp_source);
    let lineitem = ctx.stream(lineitem_source);
    let nation = ctx.stream(nation_source);


    let availqty_threshold = lineitem
        .filter(|l_row| {
            let start_date = NaiveDate::parse_from_str("1994-01-01", "%Y-%m-%d").unwrap();
            let end_date = start_date + chrono::Duration::days(365);
            let l_shipdate = NaiveDate::parse_from_str(&l_row.l_shipdate, "%Y-%m-%d").unwrap();

            l_shipdate >= start_date && l_shipdate < end_date
        })
        .group_by_sum(|l_row| (l_row.l_partkey, l_row.l_suppkey), |l_row| l_row.l_quantity) 
        .unkey()
        .map(|((l_partkey, l_suppkey), sum)| (l_partkey, l_suppkey, 0.5 * sum));
    
    let suppkeys = partsupp
        .join(part.filter(|p_row| p_row.p_name.starts_with("forest")), 
            |ps_row| ps_row.ps_partkey, |p_row| p_row.p_partkey)
        .unkey()
        .join(availqty_threshold, |(_, (ps_row, _))| (ps_row.ps_partkey, ps_row.ps_suppkey), |(l_partkey, l_suppkey, _)| (*l_partkey, *l_suppkey))
        .unkey()
        .filter(|((_, _), ((_, (ps_row, _)), (_, _, sum)))| ps_row.ps_availqty as f64 > *sum)
        .map(|((_, _), ((_, (ps_row, _)), (_, _, _)))| ps_row.ps_suppkey);
 
    supplier
        .join(nation.filter(|n_row| n_row.n_name == "CANADA"), |s_row| s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(suppkeys, |(_, (s_row, _))| s_row.s_suppkey, |suppkey| *suppkey)
        .unkey()
        .map(|(_, ((_, (s_row, _)), _))| (s_row.s_name, s_row.s_address))
        .collect_vec();

    ctx
    
} 

// query 21
fn query21(config:RuntimeConfig, input_path:&str) -> StreamContext{
    
    let ctx = StreamContext::new(config.clone());

    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", input_path));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", input_path));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", input_path));
    
    
    let mut lineitem = ctx.stream(lineitem_source).split(3);    
    let orders = ctx.stream(orders_source);
    let supplier = ctx.stream(supplier_source);
    let nation = ctx.stream(nation_source);

    supplier
        .join(lineitem.pop().unwrap(), |s_row| s_row.s_suppkey, |l_row| l_row.l_suppkey)
        .unkey()
        .join(orders.filter(|o_row| o_row.o_orderstatus == "F"), |(_, (_, l_row))| l_row.l_orderkey, |o_row| o_row.o_orderkey)
        .unkey()
        .join(nation.filter(|n_row| n_row.n_name == "SAUDI ARABIA"), |(_, ((_, (s_row, _)), _))|  s_row.s_nationkey, |n_row| n_row.n_nationkey)
        .unkey()
        .join(lineitem.pop().unwrap(), |(_, ((_, ((_, (_, l_row)), _)), _))| l_row.l_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .left_join(lineitem.pop().unwrap().filter(|l_row| {
            let l_receiptdate = NaiveDate::parse_from_str(&l_row.l_receiptdate, "%Y-%m-%d").unwrap();
            let l_commitdate = NaiveDate::parse_from_str(&l_row.l_commitdate, "%Y-%m-%d").unwrap();

            l_receiptdate > l_commitdate
        }), |(_, ((_, ((_, ((_, (_, l_row)), _)), _)), _))| l_row.l_orderkey, |l_row| l_row.l_orderkey)
        .unkey()
        .filter(|(_, ((_, ((_, ((_, ((_, (_, l1)), _)), _)), l2)), l3))| {        
            l2.l_suppkey != l1.l_suppkey &&
            l3.is_none()
        })
        .group_by_count(|(_, ((_, ((_, ((_, ((_, (s_row, _)), _)), _)), _)), _))| s_row.s_name.clone())
        .collect_vec();
        
    ctx
    
} 


// query 22
fn query22(config:RuntimeConfig, input_path:&str) -> StreamContext{

    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", input_path));    
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", input_path));
    

    // first part to calcultate the average value
    let ctx = StreamContext::new(config.clone());
    let customer = ctx.stream(customer_source.clone());

    #[derive(Clone, Default, Deserialize, Serialize)]
    struct Aggregates {
        sum : f64,
        count: i32
    }

    let c_acctbal_avg = customer
        .filter(|c_row| {
            let prefixes = vec!["13", "31", "23", "29", "30", "18", "17"];
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

    let custsale = customer
        .filter(move |c_row| {
            let prefixes = vec!["13", "31", "23", "29", "30", "18", "17"];

            prefixes.contains(&(&(c_row.c_phone.to_string())[0..2])) &&
            c_row.c_acctbal > c_acctbal_avg_value
        })
        .left_join(orders, |c_row| c_row.c_custkey, |o_row| o_row.o_custkey)
        .filter(|(_, (_, option_row))| option_row.is_none())
        .unkey()
        .map(|(_, (c_row, _))| ((&c_row.c_phone[0..2]).to_string(), c_row.c_acctbal));

    
    custsale
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
        
    ctx2
    
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
    

    let mut query_number = 1;
    for query in queries {
        let start = std::time::Instant::now();        
        let ctx = query(config.clone(), input_path.as_str());
        ctx.execute_blocking();
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
    let part_source = CsvSource::<PartRow>::new(format!("{}part.csv", INPUT_PATH));
    let supplier_source = CsvSource::<SupplierRow>::new(format!("{}supplier.csv", INPUT_PATH));
    let partsupp_source = CsvSource::<PartsuppRow>::new(format!("{}partsupp.csv", INPUT_PATH));
    let customer_source = CsvSource::<CustomerRow>::new(format!("{}customer.csv", INPUT_PATH));
    let orders_source = CsvSource::<OrdersRow>::new(format!("{}orders.csv", INPUT_PATH));    
    let lineitem_source = CsvSource::<LineitemRow>::new(format!("{}lineitem.csv", INPUT_PATH));
    let nation_source = CsvSource::<NationRow>::new(format!("{}nation.csv", INPUT_PATH));
    let region_source = CsvSource::<RegionRow>::new(format!("{}region.csv", INPUT_PATH));

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