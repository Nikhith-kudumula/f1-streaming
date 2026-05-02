package com.f1streaming.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PitStop {

    @JsonProperty("session_key")
    private int sessionKey;

    @JsonProperty("driver_number")
    private int driverNumber;

    @JsonProperty("lap_number")
    private int lapNumber;

    @JsonProperty("pit_duration")
    private Double pitDuration;

    @JsonProperty("date")
    private String date;

    public int getSessionKey() { return sessionKey; }
    public void setSessionKey(int sessionKey) { this.sessionKey = sessionKey; }

    public int getDriverNumber() { return driverNumber; }
    public void setDriverNumber(int driverNumber) { this.driverNumber = driverNumber; }

    public int getLapNumber() { return lapNumber; }
    public void setLapNumber(int lapNumber) { this.lapNumber = lapNumber; }

    public Double getPitDuration() { return pitDuration; }
    public void setPitDuration(Double pitDuration) { this.pitDuration = pitDuration; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }
}
