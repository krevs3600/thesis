package com.thesis.flink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class TCP_scenario {
    
    private final TableEnvironment tableEnvironment;
    private static List<String> queries;

    public TCP_scenario(TableEnvironment tableEnvironment){
        this.tableEnvironment = tableEnvironment;
        TCP_scenario.queries = loadQueries();
        initDatabase();
    }

    private static List<String> loadQueries(){
        String queriesPath = System.getenv("QUERIES_PATH");
        if (queriesPath == null || queriesPath.isEmpty()) {
            System.out.println("QUERIES_PATH is not set.");
            return new ArrayList<>();
        }

        List<String> queryList = new ArrayList<>();
        try {
            // List all files in the directory
            File dir = new File(queriesPath);
            File[] files = dir.listFiles((directory, fileName) -> fileName.endsWith(".sql"));
            
            if (files != null) {
                // Loop over the files and read them
                for (File file : files) {
                    // Read the content of the file
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        StringBuilder fileContent = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            fileContent.append(line).append("\n");
                        }
                        // Add file content with id to the list
                        queryList.add(fileContent.toString());
                    } catch (IOException e) {
                        System.out.println("Error reading file: " + file.getName());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error accessing the directory: " + queriesPath);
        }
        
        return queryList;

    }

    private void initDatabase(){

        String inputPath = System.getenv("INPUT_PATH");
        if (inputPath == null || inputPath.isEmpty()) {
            System.out.println("QUERIES_PATH is not set.");
            return;
        }

        // ----- PART -----
        TableDescriptor partDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("p_partkey", DataTypes.INT())
                .column("p_name", DataTypes.STRING())
                .column("p_mfgr", DataTypes.STRING())
                .column("p_brand", DataTypes.STRING())
                .column("p_type", DataTypes.STRING())
                .column("p_size", DataTypes.INT())
                .column("p_container", DataTypes.STRING())
                .column("p_retailprice", DataTypes.DECIMAL(10,2))
                .column("p_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/part.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- SUPPLIER -----
        TableDescriptor supplierDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("s_suppkey", DataTypes.INT())
                .column("s_name", DataTypes.STRING())
                .column("s_address", DataTypes.STRING())
                .column("s_nationkey", DataTypes.INT())
                .column("s_phone", DataTypes.STRING())
                .column("s_acctbal", DataTypes.DECIMAL(10,2))
                .column("s_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/supplier.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- PARTSUPP -----
        TableDescriptor partsuppDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("ps_partkey", DataTypes.INT())
                .column("ps_suppkey", DataTypes.INT())
                .column("ps_availqty", DataTypes.INT())
                .column("ps_supplycost", DataTypes.DECIMAL(10,2))
                .column("ps_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/partsupp.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- CUSTOMER -----
        TableDescriptor customerDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("c_custkey", DataTypes.INT())
                .column("c_name", DataTypes.STRING())
                .column("c_address", DataTypes.STRING())
                .column("c_nationkey", DataTypes.INT())
                .column("c_phone", DataTypes.STRING())
                .column("c_acctbal", DataTypes.DECIMAL(10,2))
                .column("c_mktsegment", DataTypes.STRING())
                .column("c_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/customer.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- ORDERS -----
        TableDescriptor ordersDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("o_orderkey", DataTypes.INT())
                .column("o_custkey", DataTypes.INT())
                .column("o_orderstatus", DataTypes.STRING())
                .column("o_totalprice", DataTypes.DECIMAL(10,2))
                .column("o_orderdate", DataTypes.DATE())
                .column("o_orderpriority", DataTypes.STRING())
                .column("o_clerk", DataTypes.STRING())
                .column("o_shippriority", DataTypes.INT())
                .column("o_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/orders.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- LINEITEM -----
        TableDescriptor lineitemDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("l_orderkey", DataTypes.INT())
                .column("l_partkey", DataTypes.INT())
                .column("l_suppkey", DataTypes.INT())
                .column("l_linenumber", DataTypes.INT())
                .column("l_quantity", DataTypes.DECIMAL(10,2))
                .column("l_extendedprice", DataTypes.DECIMAL(10,2))
                .column("l_discount", DataTypes.DECIMAL(10,2))
                .column("l_tax", DataTypes.DECIMAL(10,2))
                .column("l_returnflag", DataTypes.STRING())
                .column("l_linestatus", DataTypes.STRING())
                .column("l_shipdate", DataTypes.DATE())
                .column("l_commitdate", DataTypes.DATE())
                .column("l_receiptdate", DataTypes.DATE())
                .column("l_shipinstruct", DataTypes.STRING())
                .column("l_shipmode", DataTypes.STRING())
                .column("l_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/lineitem.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- NATION -----
        TableDescriptor nationDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("n_nationkey", DataTypes.INT())
                .column("n_name", DataTypes.STRING())
                .column("n_regionkey", DataTypes.INT())
                .column("n_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/nation.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // ----- REGION -----
        TableDescriptor regionDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("r_regionkey", DataTypes.INT())
                .column("r_name", DataTypes.STRING())
                .column("r_comment", DataTypes.STRING())
                .build())
            .option("path", "file:////" + inputPath + "/region.csv")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("skip-first-line", "true") 
                .option("field-delimiter", ",")
                .build())
            .build();

        // Create temporary tables
        tableEnvironment.createTemporaryTable("part", partDescriptor);
        tableEnvironment.createTemporaryTable("supplier", supplierDescriptor);
        tableEnvironment.createTemporaryTable("partsupp", partsuppDescriptor);
        tableEnvironment.createTemporaryTable("customer", customerDescriptor);
        tableEnvironment.createTemporaryTable("orders", ordersDescriptor);
        tableEnvironment.createTemporaryTable("lineitem", lineitemDescriptor);
        tableEnvironment.createTemporaryTable("nation", nationDescriptor);
        tableEnvironment.createTemporaryTable("region", regionDescriptor);
    }

    public Table executeQuery() {
        Table data = tableEnvironment.from("nation");
        Table out = data.select($("*"));
        return out;
    }

    public void executeQuery(String query) {
        TableResult res = tableEnvironment.sqlQuery(query).execute();
        int i = 0;
        CloseableIterator<Row> it = res.collect();
        while (it.hasNext()) {
            it.next();
            i++;
        }
        
    }

    public List<String> getQueries() {
        return queries;
    }


    public static void runTest() throws SQLException {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        TCP_scenario scenario = new TCP_scenario(tEnv);
  

        int queryNumber = 1;
        for (String q : queries){
            try {
                
                double startTime = System.currentTimeMillis();
                scenario.executeQuery(q);
                double executionTimeS= (System.currentTimeMillis() - startTime)/1000.0;
                sendExecutionTime(queryNumber, 1, executionTimeS);
                System.out.println(queryNumber + "-> exec_time " + executionTimeS);
            } catch (Exception e) {
                System.out.println("could't execute query number " +  queryNumber);
            }
            queryNumber++;
        }  
    }

    public static void sendExecutionTime(int queryId, int runId, double executionTime) {
        // Define the URL for the POST request
        String url = "http://127.0.0.1:5000/execution_time";
        String BENCHMARK = "TPC-H";
        String BACKEND = "flink";
        String TEST = "tcph_1_gb";

        // Create JSON payload
        JSONObject payload = new JSONObject();
        payload.put("benchmark", BENCHMARK);
        payload.put("backend", BACKEND);
        payload.put("test", TEST);
        payload.put("query_id", queryId);
        payload.put("run_id", runId);
        payload.put("execution_time", executionTime);

        // Create OkHttpClient
        OkHttpClient client = new OkHttpClient();

        // Create the request body with JSON payload
        RequestBody body = RequestBody.create(
                payload.toString(),
                okhttp3.MediaType.get("application/json; charset=utf-8")
        );

        // Build the POST request
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();

        // Execute the request
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                System.out.println("Execution time sent successfully: " + response.body().string());
            } else {
                System.err.println("Failed to send execution time. Response code: " + response.code());
            }
        } catch (IOException e) {
        }
    }
    
}
