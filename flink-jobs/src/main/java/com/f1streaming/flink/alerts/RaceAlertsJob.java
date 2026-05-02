package com.f1streaming.flink.alerts;

import com.f1streaming.flink.config.FlinkJobConfig;
import com.f1streaming.flink.model.RaceControlEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Job 3: Race Alerts
 * Filters race control messages for safety-relevant events (safety car, red flag,
 * yellow flag, DRS disabled) and emits structured alerts to f1.alerts.
 */
public class RaceAlertsJob {

    private static final Set<String> ALERT_FLAGS = Set.of(
            "YELLOW", "DOUBLE YELLOW", "RED", "SAFETY CAR", "VIRTUAL SAFETY CAR"
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setTopics(FlinkJobConfig.TOPIC_RACE_CONTROL)
                .setGroupId(FlinkJobConfig.getGroupId("alerts"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(FlinkJobConfig.TOPIC_ALERTS)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        DataStream<String> rcStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((json, ts) -> {
                            try {
                                RaceControlEvent e = mapper.readValue(json, RaceControlEvent.class);
                                return e.date != null
                                        ? java.time.OffsetDateTime.parse(e.date).toInstant().toEpochMilli()
                                        : ts;
                            } catch (Exception ex) { return ts; }
                        }),
                "race-control-source");

        rcStream
                .process(new AlertFilterFunction())
                .sinkTo(sink);

        env.execute("F1 Race Alerts Job");
    }

    static class AlertFilterFunction extends ProcessFunction<String, String> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void processElement(String json, Context ctx, Collector<String> out) throws Exception {
            RaceControlEvent event = mapper.readValue(json, RaceControlEvent.class);

            AlertSeverity severity = classify(event);
            if (severity == null) return;

            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "race_alert");
            alert.put("severity", severity.name());
            alert.put("flag", event.flag);
            alert.put("category", event.category);
            alert.put("message", event.message);
            alert.put("date", event.date);
            alert.put("driver_number", event.driverNumber);
            alert.put("session_key", event.sessionKey);

            out.collect(mapper.writeValueAsString(alert));
        }

        private AlertSeverity classify(RaceControlEvent event) {
            String flag = event.flag != null ? event.flag.toUpperCase() : "";
            String msg  = event.message != null ? event.message.toUpperCase() : "";

            if (flag.contains("RED") || msg.contains("RED FLAG"))           return AlertSeverity.CRITICAL;
            if (flag.contains("SAFETY CAR") || msg.contains("SAFETY CAR")) return AlertSeverity.HIGH;
            if (flag.contains("VIRTUAL SAFETY CAR"))                        return AlertSeverity.HIGH;
            if (flag.contains("DOUBLE YELLOW") || flag.contains("YELLOW")) return AlertSeverity.MEDIUM;
            if (msg.contains("DRS DISABLED"))                               return AlertSeverity.LOW;

            return null;
        }
    }

    enum AlertSeverity { CRITICAL, HIGH, MEDIUM, LOW }
}
