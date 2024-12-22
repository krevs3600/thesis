package com.thesis.flink;


import com.thesis.flink.job.AvgJob;
import com.thesis.flink.job.FillNaJob;
import com.thesis.flink.job.FilterAndSumJob;
import com.thesis.flink.job.FilterNaJob;
import com.thesis.flink.job.FilterWithCompJob;
import com.thesis.flink.job.Job;



public class App {
    public static void main(String[] args) throws Exception{
        Job fill_na = new FillNaJob();
        Job filter_na = new FilterNaJob();
        Job filter_with_comp = new FilterWithCompJob();
        Job filter_and_sum = new FilterAndSumJob();
        Job avg_job = new AvgJob();
        filter_na.run();
        fill_na.run();
        filter_with_comp.run();
        filter_and_sum.run();
        avg_job.run();
    }
}