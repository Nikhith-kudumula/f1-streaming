package com.f1streaming.flink.leaderboard;

import com.f1streaming.flink.config.FlinkJobConfig;
import com.f1streaming.flink.model.PositionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Job 1: Leaderboard
 * Aggregates driver positions in 10-second tumbling windows and emits a ranked snapshot.
 */
public class LeaderboardJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setTopics(FlinkJobConfig.TOPIC_POSITIONS)
                .setGroupId(FlinkJobConfig.getGroupId("leaderboard"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(FlinkJobConfig.TOPIC_LEADERBOARD)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        ObjectMapper mapper = new ObjectMapper();

        DataStream<String> positionStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((json, ts) -> {
                            try {
                                PositionEvent e = mapper.readValue(json, PositionEvent.class);
                                return e.date != null
                                        ? java.time.OffsetDateTime.parse(e.date).toInstant().toEpochMilli()
                                        : ts;
                            } catch (Exception ex) { return ts; }
                        }),
                "f1-positions-source");

        positionStream
                .keyBy(json -> "global")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new LeaderboardAggregator())
                .sinkTo(sink);

        env.execute("F1 Leaderboard Job");
    }

    static class LeaderboardAggregator
            implements AggregateFunction<String, Map<Integer, Integer>, String> {

        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Map<Integer, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<Integer, Integer> add(String json, Map<Integer, Integer> acc) {
            try {
                PositionEvent e = mapper.readValue(json, PositionEvent.class);
                acc.put(e.driverNumber, e.position);
            } catch (Exception ignored) {}
            return acc;
        }

        @Override
        public String getResult(Map<Integer, Integer> acc) {
            // Sort by position value, emit as JSON leaderboard snapshot
            TreeMap<Integer, Integer> sorted = new TreeMap<>();
            acc.forEach((driver, pos) -> sorted.put(pos, driver));
            try {
                return mapper.writeValueAsString(Map.of(
                        "type", "leaderboard",
                        "rankings", sorted
                ));
            } catch (Exception e) {
                return "{}";
            }
        }

        @Override
        public Map<Integer, Integer> merge(Map<Integer, Integer> a, Map<Integer, Integer> b) {
            a.putAll(b);
            return a;
        }
    }
}
