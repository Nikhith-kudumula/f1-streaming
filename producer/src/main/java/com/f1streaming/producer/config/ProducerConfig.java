package com.f1streaming.producer.config;

public class ProducerConfig {

    public static final String OPENF1_BASE_URL = "https://api.openf1.org/v1";

    // session_key for the 2023 Monaco GP Race — good demo session
    public static final int DEFAULT_SESSION_KEY = 9161;

    // Replay speed multiplier (2.0 = 2x faster than real time)
    public static final double REPLAY_SPEED = 2.0;

    public static final String TOPIC_POSITIONS     = "f1.positions";
    public static final String TOPIC_LAPS          = "f1.laps";
    public static final String TOPIC_PIT_STOPS     = "f1.pit_stops";
    public static final String TOPIC_RACE_CONTROL  = "f1.race_control";

    private final String bootstrapServers;
    private final int sessionKey;
    private final double replaySpeed;

    public ProducerConfig(String bootstrapServers, int sessionKey, double replaySpeed) {
        this.bootstrapServers = bootstrapServers;
        this.sessionKey = sessionKey;
        this.replaySpeed = replaySpeed;
    }

    public static ProducerConfig fromEnv() {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int sessionKey = Integer.parseInt(System.getenv().getOrDefault("SESSION_KEY", String.valueOf(DEFAULT_SESSION_KEY)));
        double replaySpeed = Double.parseDouble(System.getenv().getOrDefault("REPLAY_SPEED", String.valueOf(REPLAY_SPEED)));
        return new ProducerConfig(bootstrapServers, sessionKey, replaySpeed);
    }

    public String getBootstrapServers() { return bootstrapServers; }
    public int getSessionKey() { return sessionKey; }
    public double getReplaySpeed() { return replaySpeed; }
}
