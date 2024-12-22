package com.thesis.flink.job;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.$;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;


public class FilterNaJob extends Job{
    private final TableDescriptor outTableDescriptor;
    private DataStream<CsvRow> filteredStream;

    public FilterNaJob() throws Exception{
        super();
        outTableDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("int1", DataTypes.INT())
                .column("string1", DataTypes.STRING())
                .column("int4", DataTypes.INT())
                .build())
            .option("path", getOutputPath() + "/query_0")
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
        //getTableEnvironment().executeSql("SELECT * FROM csvTable").print();
        Table outTable = data.filter($("int4").isNotNull());
        outTable.executeInsert(outTableDescriptor);
    }

    @Override
    public void writeToCsv(Path path) {
        FileSink<CsvRow> fileSink = FileSink.forRowFormat((path), new SimpleStringEncoder<CsvRow>())
                .build();
        filteredStream.sinkTo(fileSink);
    }
}
