package com.f1streaming.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RaceControlMessage {

    @JsonProperty("session_key")
    private int sessionKey;

    @JsonProperty("message")
    private String message;

    @JsonProperty("category")
    private String category;

    @JsonProperty("flag")
    private String flag;

    @JsonProperty("date")
    private String date;

    @JsonProperty("driver_number")
    private Integer driverNumber;

    public int getSessionKey() { return sessionKey; }
    public void setSessionKey(int sessionKey) { this.sessionKey = sessionKey; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getFlag() { return flag; }
    public void setFlag(String flag) { this.flag = flag; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public Integer getDriverNumber() { return driverNumber; }
    public void setDriverNumber(Integer driverNumber) { this.driverNumber = driverNumber; }
}
