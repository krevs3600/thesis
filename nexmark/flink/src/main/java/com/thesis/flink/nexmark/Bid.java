package com.thesis.flink.nexmark;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Bid {
    public long idx;
    public long auction;
    public long bidder;
    public long price;
    public String channel;
    public String url;
    public Instant date_time; 
    public String extra;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void setDate_time(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, FORMATTER);
        this.date_time = localDateTime.toInstant(ZoneOffset.UTC);
    }
}