package com.thesis.flink.nexmark;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Auction {
    public long idx;
    public long id;
    public String item_name;
    public String description;
    public long initial_bid;
    public long reserve;
    public Instant date_time; 
    public Instant expires; 
    public long seller;
    public int category;
    public String extra;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void setDate_time(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, FORMATTER);
        this.date_time = localDateTime.toInstant(ZoneOffset.UTC);
    }

    public void setExpires(String expires) {
        LocalDateTime localDateTime = LocalDateTime.parse(expires, FORMATTER);
        this.expires = localDateTime.toInstant(ZoneOffset.UTC);
    }
}