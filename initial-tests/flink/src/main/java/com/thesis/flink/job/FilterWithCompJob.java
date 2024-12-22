package com.thesis.flink.job;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.and;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;


public class FilterWithCompJob extends Job{

    private DataStream<CsvRow> filledStream;
    private final TableDescriptor outTableDescriptor;

    public FilterWithCompJob() throws Exception{
        super();
        outTableDescriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("int1", DataTypes.INT())
                .column("string1", DataTypes.STRING())
                .column("int4", DataTypes.INT())
                .build())
            .option("path", getOutputPath() + "/query_2")
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
        /*
        filledStream = getInputStream().filter((CsvRow row) -> (row.int4 != null && row.int4 % 2 == 0));
        writeToCsv(new Path(getAppPath()+"/output_filter_with_comp"));
        getStreamExecutionEnvironment().execute();
        */
        Table data = getTableEnvironment().from("csvTable");
        Table outTable = data.filter(
            and(
                $("int4").isNotNull(),
                $("int4").mod(2).isEqual(0)
            ));

        outTable.executeInsert(outTableDescriptor);
    }



    @Override
    public void writeToCsv(Path path) {
        FileSink<CsvRow> fileSink = FileSink.forRowFormat((path), new SimpleStringEncoder<CsvRow>())
                .build();
        filledStream.sinkTo(fileSink);
    }
    
    
}
