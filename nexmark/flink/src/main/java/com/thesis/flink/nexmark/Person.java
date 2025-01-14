package com.thesis.flink.nexmark;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Person {
    public long idx;
    public long id;
    public String name;
    public String email_address;
    public String credit_card;
    public String city;
    public String state;
    public Instant date_time;
    public String extra;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void setDate_time(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, FORMATTER);
        this.date_time = localDateTime.toInstant(ZoneOffset.UTC);
    }
}
