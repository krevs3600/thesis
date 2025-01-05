package com.thesis.flink;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
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
    final DataStream<Person> personStream;
    final DataStream<Auction> auctionStream;
    final DataStream<Bid> bidStream;

    
    public NexmarkTest () {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tableEnv = StreamTableEnvironment.create(env);
        
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

        // Create DataStreams/ Define the join logic
        personStream = env.fromSource(personSource, WatermarkStrategy.noWatermarks(), "Person Stream");
        auctionStream = env.fromSource(auctionSource, WatermarkStrategy.noWatermarks(), "Auction Stream");
        bidStream = env.fromSource(bidSource, WatermarkStrategy.noWatermarks(), "Bid Stream");

        tableEnv.createTemporaryView(
            "bid", 
            bidStream,
            Schema.newBuilder()
                .column("auction", DataTypes.BIGINT())
                .column("bidder", DataTypes.BIGINT())
                .column("price", DataTypes.BIGINT())
                .column("channel", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .column("date_time", DataTypes.BIGINT()) 
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .column("extra", DataTypes.STRING())
                .build()
        );

        tableEnv.createTemporaryView(
            "auction", 
            auctionStream,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT())               
                .column("item_name", DataTypes.STRING())        
                .column("description", DataTypes.STRING())      
                .column("initial_bid", DataTypes.BIGINT())      
                .column("reserve", DataTypes.BIGINT())          
                .column("date_time", DataTypes.BIGINT())
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")  
                .column("expires", DataTypes.BIGINT())  
                .column("seller", DataTypes.BIGINT())           
                .column("category", DataTypes.INT())         
                .column("extra", DataTypes.STRING())            
                .build()
        );

        tableEnv.createTemporaryView(
            "person", 
            personStream,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT())               
                .column("name", DataTypes.STRING())             
                .column("email_address", DataTypes.STRING())    
                .column("credit_card", DataTypes.STRING())      
                .column("city", DataTypes.STRING())             
                .column("state", DataTypes.STRING())            
                .column("date_time", DataTypes.BIGINT())
                .column("extra", DataTypes.STRING())
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(date_time, 3)") // Derived TIMESTAMP column
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")             
                .build()
        );
        
    }
    
    
    public TableResult sqlQuery1(){
        return tableEnv.executeSql("SELECT\n" + //
                        "    auction,\n" + //
                        "    bidder,\n" + //
                        "    0.908 * price as price, -- convert dollar to euro\n" + //
                        "    event_time ,\n" + //
                        "    extra\n" + //
                        "FROM bid;");
    }

    public TableResult sqlQuery2(){
        return tableEnv.executeSql("SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0;");
    }

    public TableResult sqlQuery3(){
        return tableEnv.executeSql("SELECT" + //
        "   P.name, P.city, P.state, A.id" + //
        "   FROM" + //
        "   auction AS A INNER JOIN person AS P on A.seller = P.id" + //
        "   WHERE" + //
        "   A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');");
    }

    public TableResult sqlQuery4(){
        return tableEnv.executeSql("SELECT" + //
        "   Q.category," + //
        "   AVG(Q.final)" + //
        "   FROM (" + //
        "   SELECT MAX(B.price) AS final, A.category" + //
        "   FROM auction A, bid B" + //
        "   WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires" + // 
        "   GROUP BY A.id, A.category" + //
        "   ) Q" + //
        "   GROUP BY Q.category;");
    }

    public TableResult sqlQuery5(){
        return tableEnv.executeSql(  
        
        "SELECT AuctionBids.auction, AuctionBids.num" + //
        " FROM (" + //
        "   SELECT" + //
        "     auction," + //
        "     count(*) AS num," + //
        "     window_start AS starttime," + //
        "     window_end AS endtime" + //
        "     FROM TABLE(" + //
        "             HOP(TABLE bid, DESCRIPTOR(event_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND))" + //
        "     GROUP BY auction, window_start, window_end" + //
        " ) AS AuctionBids" + //
        " JOIN (" + //
        "   SELECT" + //
        "     max(CountBids.num) AS maxn," + //
        "     CountBids.starttime," + //
        "     CountBids.endtime" + //
        "   FROM (" + //
        "     SELECT" + //
        "       count(*) AS num," + //
        "       window_start AS starttime," + //
        "       window_end AS endtime" + //
        "     FROM TABLE(" + //
        "                HOP(TABLE bid, DESCRIPTOR(event_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND))" + //
        "     GROUP BY auction, window_start, window_end" + //
        "     ) AS CountBids" + //
        "   GROUP BY CountBids.starttime, CountBids.endtime" + //
        " ) AS MaxBids" + //
        " ON AuctionBids.starttime = MaxBids.starttime AND" + //
        "    AuctionBids.endtime = MaxBids.endtime AND" + //
        "    AuctionBids.num >= MaxBids.maxn;");
    }

    // not supported by flink
    public TableResult sqlQuery6(){
        return tableEnv.executeSql("SELECT " + //
        "    Q.seller, " + //
        "    AVG(Q.price) OVER (" + //
        "        PARTITION BY Q.seller " + //
        "        ORDER BY Q.date_time " + //
        "        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW" + //
        "    ) AS avg_price" + //
        "FROM (" + //
        "    SELECT * " + //
        "    FROM (" + //
        "        SELECT " + //
        "            A.id, " + //
        "            A.seller, " + //
        "            B.price, " + //
        "            B.date_time, " + //
        "            ROW_NUMBER() OVER (" + //
        "                PARTITION BY A.id, A.seller " + //
        "                ORDER BY B.price DESC" + //
        "            ) AS rownum" + //
        "        FROM " + //
        "            auction AS A" + //
        "        INNER JOIN " + //
        "            bid AS B" + //
        "        ON " + //
        "            A.id = B.auction " + //
        "        WHERE " + //
        "            B.date_time BETWEEN A.date_time AND A.expires" + //
        "    ) AS SubQ" + //
        "    WHERE rownum <= 1" + //
        ") AS Q;");
    }

    public TableResult sqlQuery7(){
        return tableEnv.executeSql("SELECT B.auction, B.price, B.bidder, B.event_time, B.extra " + //
            "FROM bid B " + //
            "JOIN (" + //
            "  SELECT MAX(price) AS maxprice, window_end as date_time" + //
            "  FROM TABLE(" + //
            "          TUMBLE(TABLE bid, DESCRIPTOR(event_time), INTERVAL '10' SECOND))" + //
            "  GROUP BY window_start, window_end" + //
            ") B1 " + //
            "ON B.price = B1.maxprice " + //
            "WHERE TO_TIMESTAMP_LTZ(B.date_time,3) BETWEEN B1.date_time  - INTERVAL '10' SECOND AND B1.date_time;");
    }

    public TableResult sqlQuery8(){
        return tableEnv.executeSql("SELECT P.id, P.name, P.starttime" + //
            "FROM (" + //
            "  SELECT id, name," + //
            "        window_start AS starttime," + //
            "        window_end AS endtime" + //
            "  FROM TABLE(" + //
            "            TUMBLE(TABLE person, DESCRIPTOR(date_time), INTERVAL '10' SECOND))" + //
            "  GROUP BY id, name, window_start, window_end" + //
            ") P" + //
            "JOIN (" + //
            "  SELECT seller," + //
            "        window_start AS starttime," + //
            "        window_end AS endtime" + //
            "  FROM TABLE(" + //
            "        TUMBLE(TABLE auction, DESCRIPTOR(date_time), INTERVAL '10' SECOND))" + //
            "  GROUP BY seller, window_start, window_end" + //
            ") A" + //
            "ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;");
    }


    public void query1(){
        bidStream.
            map(bid -> bid.price *= 0.908);
    }

   

    public void query2(){
        bidStream.
            filter(bid -> {
                List<Integer> auctionsFilter = Arrays.asList(1020, 2001, 2019, 2087);
                return auctionsFilter.contains((int) bid.auction);
            });
    }

    public void query3(){
        auctionStream
            .join(personStream)
            .where((Auction auction) -> auction.seller)
            .equalTo((Person person) -> person.id)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .apply((auction, person) -> { 
            
                if (auction.category == 10 && 
                    (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"))) {
                        return new Tuple4<>(person.name, person.city, person.state, auction.id); 
                }
                return null;
            })
            .filter(result -> result != null)
            .print(); 
    }

    public TableEnvironment getTableEnvironment (){
        return this.tableEnv;
    }

    
}