package com.thesis.flink.job;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Abstract class for a Flink Job for a test scenario.
 * The test consist of reading a file from a csv file and writing back to it.
 * Each class extending the Job will process the data in a different way.
 * The configuration is fixed for this specific test.
 */
public abstract class Job {

    private String inputPath;
    private String outputPath;
    // for this test I'm using the Table API
    private final TableEnvironment tEnv;

    /**
     * POJO class representing the row of the csv file
     */
    @JsonPropertyOrder({"int1","string1","int4"})
    public static class CsvRow {
        public Integer int1;
        public String string1;
        public Integer int4;

        public CsvRow(){

        }
        
        public CsvRow(Integer int1, String string1, Integer int4) {
            this.int1 = int1;
            this.string1 = string1;
            this.int4 = int4;
        }

        @Override
        public String toString(){
            return int1 + "," + string1 + "," + int4;
        } 
    }

    /**
     * Get env variables, create flink enviroment and then initialize it for further processing.
     * @throws Exception
     */
    public Job() throws Exception {
        // getting env variables
        this.inputPath = System.getenv("INPUT_PATH");
        this.outputPath = System.getenv("OUTPUT_PATH");

        if (inputPath == null || outputPath == null) {
            throw new RuntimeException("Couldn't read ENV variables");
        }
        this.inputPath += "/ints_string.csv";
        this.outputPath += "/flink";

        // initializing flink enviroment
        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        this.tEnv = TableEnvironment.create(settings);
        init();
    }
    
    /**
     * Init function describing the type of data to be read from the csv
     * and to load it to the table environment.
     * @throws Exception
     */
    private void init() throws Exception {
        File inputFile;
        // check inputFile file existance
        inputFile = new File(inputPath);
        if(!inputFile.exists() || inputFile.isDirectory()) { 
            System.out.println("File not found");
            throw new FileNotFoundException("Input CSV file not found at: " + inputPath);
        }

        // descriptor of the data read from the csv file
        TableDescriptor descriptor = TableDescriptor
            .forConnector("filesystem")
            .schema(Schema.newBuilder()
                .column("int1", DataTypes.INT())
                .column("string1", DataTypes.STRING())
                .column("int4", DataTypes.INT().nullable()) // int4 can be absent
                .build())
            .option("path", inputPath)
            .format(FormatDescriptor.forFormat("csv")
                .option("field-delimiter", ",")
                .option("null-literal", "")
                .option("allow-comments", "true")
                .build()
            )
            .build();

        tEnv.createTemporaryTable("csvTable", descriptor);
        
    }

    /**
     * Processing job
     * @throws Exception
     */
    public abstract void run() throws Exception;

    
    /**
     * Write to csv the result of the run job
     * @param path
     */
    public abstract void writeToCsv(Path path);


    /**
     * getter for tableEnvironment
     * @return tableEnvironment
     */
    public TableEnvironment getTableEnvironment(){
        return tEnv;
    }

    /**
     * To get the name of the class
     * @return name of the class
     */
    public String getName(){
        return this.getClass().getName();
    }

    /**
     * To get the output base path
     * @return output base path
     */
    public String getOutputPath(){
        return this.outputPath;
    }


}