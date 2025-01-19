package com.thesis.flink;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.thesis.flink.nexmark.Auction;
import com.thesis.flink.nexmark.AuctionDeserializationSchema;
import com.thesis.flink.nexmark.Bid;
import com.thesis.flink.nexmark.BidDeserializationSchema;
import com.thesis.flink.nexmark.Person;
import com.thesis.flink.nexmark.PersonDeserializationSchema;

public class NexmarkTest {

    final StreamExecutionEnvironment env; 
    final StreamTableEnvironment tableEnv; 
    final String brokers = "localhost:19092";

    public NexmarkTest () {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();
        this.tableEnv = StreamTableEnvironment.create(env, settings);
        //initTableWithTableApi();
        initTableFromKafkaSource();
    }

    private void initTableWithTableApi() {
        String auction = readSqlFile("sources/auction.sql");
        String person = readSqlFile("sources/person.sql");
        String bid = readSqlFile("sources/bid.sql");

        tableEnv.executeSql(auction);
        tableEnv.executeSql(person);
        tableEnv.executeSql(bid);
    }

    private void initTableFromKafkaSource(){
        KafkaSource<Person> personSource = KafkaSource.<Person>builder()
            .setBootstrapServers(brokers)
            .setTopics("person-topic")
            .setGroupId("flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new PersonDeserializationSchema())
            .build();

        KafkaSource<Auction> auctionSource = KafkaSource.<Auction>builder()
            .setBootstrapServers(brokers)
            .setTopics("auction-topic")
            .setGroupId("flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new AuctionDeserializationSchema())
            .build();

        KafkaSource<Bid> bidSource = KafkaSource.<Bid>builder()
            .setBootstrapServers(brokers)
            .setTopics("bid-topic")
            .setGroupId("flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new BidDeserializationSchema())
            .build();

        // Create DataStreams
        DataStream<Person> personStream = env.fromSource(personSource, WatermarkStrategy.noWatermarks(), "Person Stream");
        DataStream<Auction> auctionStream = env.fromSource(auctionSource, WatermarkStrategy.noWatermarks(), "Auction Stream");
        DataStream<Bid> bidStream = env.fromSource(bidSource, WatermarkStrategy.noWatermarks(), "Bid Stream");

        tableEnv.createTemporaryView("bid", 
            bidStream,
            Schema.newBuilder()
                .column("idx", DataTypes.BIGINT())
                .column("auction", DataTypes.BIGINT())
                .column("bidder", DataTypes.BIGINT())
                .column("price", DataTypes.BIGINT())
                .column("channel", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("date_time", DataTypes.TIMESTAMP_LTZ(3)) 
                //.columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("date_time", "date_time - INTERVAL '1' SECOND")
                .column("extra", DataTypes.STRING())
                .build()
        );

        tableEnv.createTemporaryView(
            "auction", 
            auctionStream,
            Schema.newBuilder()
                .column("idx", DataTypes.BIGINT())
                .column("id", DataTypes.BIGINT())               
                .column("item_name", DataTypes.STRING())        
                .column("description", DataTypes.STRING())      
                .column("initial_bid", DataTypes.BIGINT())      
                .column("reserve", DataTypes.BIGINT())          
                .column("date_time", DataTypes.TIMESTAMP_LTZ(3))
                //.columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("date_time", "date_time - INTERVAL '1' SECOND")  
                .column("expires", DataTypes.TIMESTAMP_LTZ(3))  
                .column("seller", DataTypes.BIGINT())           
                .column("category", DataTypes.INT())         
                .column("extra", DataTypes.STRING())            
                .build()
        );

        tableEnv.createTemporaryView(
            "person", 
            personStream,
            Schema.newBuilder()
                .column("idx", DataTypes.BIGINT())
                .column("id", DataTypes.BIGINT())               
                .column("name", DataTypes.STRING())             
                .column("email_address", DataTypes.STRING())    
                .column("credit_card", DataTypes.STRING())      
                .column("city", DataTypes.STRING())             
                .column("state", DataTypes.STRING())            
                .column("date_time", DataTypes.TIMESTAMP_LTZ(3))
                .column("extra", DataTypes.STRING())
                //.columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("date_time", "date_time - INTERVAL '1' SECOND")             
                .build()
        ); 
    }
    
    public static String readSqlFile(String resourcePath) {
        // Get the class loader for loading the resource
        ClassLoader classLoader = NexmarkTest.class.getClassLoader();

        // Load the resource as an InputStream
        try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }

            // Convert the InputStream to a String
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
            catch (IOException e) {
                System.out.println("Error reading file: " + resourcePath);
                System.exit(0);
            }
        }
        catch (Exception e) {
            System.out.print(e.toString());
            System.exit(0);
        }

        return "";
    }


    
    public void execQuery(int query_number){
        String drop_sink = "DROP TABLE IF EXISTS kafka_sink";
        String query = readSqlFile(String.format("queries/query%d.sql", query_number));
        String kafka_sink = readSqlFile(String.format("sinks/query%d.sql", query_number));
        
        tableEnv.executeSql(drop_sink);
        tableEnv.executeSql(kafka_sink);
        tableEnv.executeSql("INSERT INTO kafka_sink " + query);
    }

    public TableEnvironment getTableEnvironment (){
        return this.tableEnv;
    }

    
}