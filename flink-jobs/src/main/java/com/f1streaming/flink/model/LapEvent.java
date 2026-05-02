package com.f1streaming.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LapEvent {

    @JsonProperty("session_key")
    public int sessionKey;

    @JsonProperty("driver_number")
    public int driverNumber;

    @JsonProperty("lap_number")
    public int lapNumber;

    @JsonProperty("lap_duration")
    public Double lapDuration;

    @JsonProperty("date_start")
    public String dateStart;

    @JsonProperty("is_pit_out_lap")
    public boolean pitOutLap;
}
