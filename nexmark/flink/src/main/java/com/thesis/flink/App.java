package com.thesis.flink;

import org.apache.flink.table.api.TableResult;

public class App {
    public static void main(String[] args) throws Exception{
        
        NexmarkTest nexmark = new NexmarkTest();
        TableResult query = nexmark.sqlQuery1();
        
        query.print();
    }
}