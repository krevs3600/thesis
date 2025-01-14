package com.thesis.flink;

public class App {
    public static void main(String[] args) throws Exception{
        
        NexmarkTest nexmark = new NexmarkTest();
        nexmark.execQuery(7);
    }
}