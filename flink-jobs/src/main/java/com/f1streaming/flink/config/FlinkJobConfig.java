package com.f1streaming.flink.config;

public class FlinkJobConfig {

    public static final String TOPIC_POSITIONS    = "f1.positions";
    public static final String TOPIC_LAPS         = "f1.laps";
    public static final String TOPIC_PIT_STOPS    = "f1.pit_stops";
    public static final String TOPIC_RACE_CONTROL = "f1.race_control";

    public static final String TOPIC_LEADERBOARD  = "f1.leaderboard";
    public static final String TOPIC_LAPS_ENRICHED = "f1.laps_enriched";
    public static final String TOPIC_ALERTS       = "f1.alerts";

    public static String getBootstrapServers() {
        return System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    }

    public static String getGroupId(String jobName) {
        return "f1-flink-" + jobName;
    }
}
