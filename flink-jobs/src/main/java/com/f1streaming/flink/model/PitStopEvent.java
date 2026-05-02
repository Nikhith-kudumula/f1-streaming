package com.f1streaming.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PitStopEvent {

    @JsonProperty("session_key")
    public int sessionKey;

    @JsonProperty("driver_number")
    public int driverNumber;

    @JsonProperty("lap_number")
    public int lapNumber;

    @JsonProperty("pit_duration")
    public Double pitDuration;

    @JsonProperty("date")
    public String date;
}
