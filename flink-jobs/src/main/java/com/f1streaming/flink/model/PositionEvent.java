package com.f1streaming.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PositionEvent {

    @JsonProperty("session_key")
    public int sessionKey;

    @JsonProperty("driver_number")
    public int driverNumber;

    @JsonProperty("position")
    public int position;

    @JsonProperty("date")
    public String date;
}
