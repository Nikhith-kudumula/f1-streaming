package com.f1streaming.producer.service;

import com.f1streaming.producer.config.ProducerConfig;
import com.f1streaming.producer.model.DriverPosition;
import com.f1streaming.producer.model.LapTime;
import com.f1streaming.producer.model.PitStop;
import com.f1streaming.producer.model.RaceControlMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OpenF1Client {

    private static final Logger log = LoggerFactory.getLogger(OpenF1Client.class);

    private final OkHttpClient http;
    private final ObjectMapper mapper;
    private final int sessionKey;

    public OpenF1Client(int sessionKey) {
        this.sessionKey = sessionKey;
        this.http = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public List<DriverPosition> fetchPositions() {
        return fetch("/position?session_key=" + sessionKey, new TypeReference<>() {});
    }

    public List<LapTime> fetchLaps() {
        return fetch("/laps?session_key=" + sessionKey, new TypeReference<>() {});
    }

    public List<PitStop> fetchPitStops() {
        return fetch("/pit?session_key=" + sessionKey, new TypeReference<>() {});
    }

    public List<RaceControlMessage> fetchRaceControl() {
        return fetch("/race_control?session_key=" + sessionKey, new TypeReference<>() {});
    }

    private <T> List<T> fetch(String path, TypeReference<List<T>> typeRef) {
        String url = ProducerConfig.OPENF1_BASE_URL + path;
        Request request = new Request.Builder().url(url).build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful() || response.body() == null) {
                log.warn("Non-success response from {}: {}", url, response.code());
                return Collections.emptyList();
            }
            return mapper.readValue(response.body().string(), typeRef);
        } catch (IOException e) {
            log.error("Failed to fetch {}", url, e);
            return Collections.emptyList();
        }
    }
}
