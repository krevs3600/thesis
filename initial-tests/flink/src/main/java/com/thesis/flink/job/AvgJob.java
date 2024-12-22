package com.thesis.flink.job;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;


public class AvgJob extends Job{

    private DataStream<RowOutput> filteredAndSummedStream;
    private final TableDescriptor outTableDescriptor;
    
    @JsonPropertyOrder({"string1","avg","count"})
    public static class RowOutput {
        private String string1;
        private Double avg;
        private Integer count;

        public RowOutput(){

        }
            
        public RowOutput(String string1, Double avg, Integer count){
            this.string1 = string1;
            this.avg = avg;  
            this.count = count;          
        }

        @Override
        public String toString(){
            return string1 + "," + avg + "," + count;
        } 
    }

    public AvgJob() throws Exception{
        super();
        outTableDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("string1", DataTypes.STRING())
                .column("sum", DataTypes.INT())
                .build())
            .option("path", getOutputPath() +"/query_4")
            .format(FormatDescriptor.forFormat("csv")
                .option("header", "true")
                .option("field-delimiter", ",")
                .option("null-literal", "")
                .option("file-extension", ".csv") 
                .build())
            .build();
    }



    @Override
    public void run() throws Exception{
        Table data = getTableEnvironment().from("csvTable");
        Table outTable = data
            .select(
                $("int1"),
                $("string1"),
                $("int4").ifNull(0).as("int4")
            )
            .groupBy($("string1"))
            .select($("string1"), $("int4").avg().as("avg"));

        outTable.executeInsert(outTableDescriptor);
    }

    @Override
    public void writeToCsv(Path path) {
        FileSink<RowOutput> fileSink = FileSink.forRowFormat((path), new SimpleStringEncoder<RowOutput>())
                .build();
        filteredAndSummedStream.sinkTo(fileSink);
    }
}
