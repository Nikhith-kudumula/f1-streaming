package com.f1streaming.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RaceControlEvent {

    @JsonProperty("session_key")
    public int sessionKey;

    @JsonProperty("message")
    public String message;

    @JsonProperty("category")
    public String category;

    @JsonProperty("flag")
    public String flag;

    @JsonProperty("date")
    public String date;

    @JsonProperty("driver_number")
    public Integer driverNumber;
}
