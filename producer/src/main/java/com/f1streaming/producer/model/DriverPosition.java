package com.f1streaming.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DriverPosition {

    @JsonProperty("session_key")
    private int sessionKey;

    @JsonProperty("driver_number")
    private int driverNumber;

    @JsonProperty("position")
    private int position;

    @JsonProperty("date")
    private String date;

    public int getSessionKey() { return sessionKey; }
    public void setSessionKey(int sessionKey) { this.sessionKey = sessionKey; }

    public int getDriverNumber() { return driverNumber; }
    public void setDriverNumber(int driverNumber) { this.driverNumber = driverNumber; }

    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }
}
