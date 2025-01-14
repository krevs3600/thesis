package com.thesis.flink;

public class App {
    public static void main(String[] args) throws Exception{
        if (args.length == 0) {
            System.err.println("No query number provided.");
            System.exit(1);
        }

        try {
            // Parse the query number from the first argument
            int queryNumber = Integer.parseInt(args[0]);
            
            if (queryNumber <= 1 || queryNumber >= 9) {
                System.err.println("Query number must me between 1 and 8");
                System.exit(1);
            }

            // Log the query number for debugging
            System.out.println("Executing query number: " + queryNumber);

            // Create an instance of NexmarkTest and execute the query
            NexmarkTest nexmark = new NexmarkTest();
            nexmark.execQuery(queryNumber);

        } catch (NumberFormatException e) {
            // Handle invalid input for the query number
            System.err.println("Invalid query number. Please provide a valid integer.");
            System.exit(1);
        }
    }
}