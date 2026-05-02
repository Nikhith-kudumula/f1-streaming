package com.f1streaming.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LapTime {

    @JsonProperty("session_key")
    private int sessionKey;

    @JsonProperty("driver_number")
    private int driverNumber;

    @JsonProperty("lap_number")
    private int lapNumber;

    @JsonProperty("lap_duration")
    private Double lapDuration;

    @JsonProperty("date_start")
    private String dateStart;

    @JsonProperty("is_pit_out_lap")
    private boolean pitOutLap;

    public int getSessionKey() { return sessionKey; }
    public void setSessionKey(int sessionKey) { this.sessionKey = sessionKey; }

    public int getDriverNumber() { return driverNumber; }
    public void setDriverNumber(int driverNumber) { this.driverNumber = driverNumber; }

    public int getLapNumber() { return lapNumber; }
    public void setLapNumber(int lapNumber) { this.lapNumber = lapNumber; }

    public Double getLapDuration() { return lapDuration; }
    public void setLapDuration(Double lapDuration) { this.lapDuration = lapDuration; }

    public String getDateStart() { return dateStart; }
    public void setDateStart(String dateStart) { this.dateStart = dateStart; }

    public boolean isPitOutLap() { return pitOutLap; }
    public void setPitOutLap(boolean pitOutLap) { this.pitOutLap = pitOutLap; }
}
