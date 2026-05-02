package com.f1streaming.flink.enrichment;

import com.f1streaming.flink.config.FlinkJobConfig;
import com.f1streaming.flink.model.LapEvent;
import com.f1streaming.flink.model.PitStopEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Job 2: Lap Enrichment
 * Joins lap events with pit stop state (keyed by driver number) to annotate
 * each lap with pit stop duration when applicable.
 */
public class LapEnrichmentJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        KafkaSource<String> lapSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setTopics(FlinkJobConfig.TOPIC_LAPS)
                .setGroupId(FlinkJobConfig.getGroupId("enrichment-laps"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> pitSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setTopics(FlinkJobConfig.TOPIC_PIT_STOPS)
                .setGroupId(FlinkJobConfig.getGroupId("enrichment-pits"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(FlinkJobConfig.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(FlinkJobConfig.TOPIC_LAPS_ENRICHED)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        WatermarkStrategy<String> watermarks = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((json, ts) -> ts);

        DataStream<String> lapStream = env.fromSource(lapSource, watermarks, "lap-source")
                .keyBy(json -> extractDriverNumber(json, mapper));

        DataStream<String> pitStream = env.fromSource(pitSource, watermarks, "pit-source")
                .keyBy(json -> extractDriverNumber(json, mapper));

        lapStream.connect(pitStream)
                .process(new LapPitJoinFunction())
                .sinkTo(sink);

        env.execute("F1 Lap Enrichment Job");
    }

    static class LapPitJoinFunction extends CoProcessFunction<String, String, String> {

        private final ObjectMapper mapper = new ObjectMapper();
        // keyed by lap_number → pit_duration
        private ValueState<Map<Integer, Double>> pitStopState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            pitStopState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pit-stops", HashMap.class));
        }

        @Override
        public void processElement1(String lapJson, Context ctx, Collector<String> out) throws Exception {
            LapEvent lap = mapper.readValue(lapJson, LapEvent.class);
            Map<Integer, Double> pitMap = pitStopState.value();

            Double pitDuration = (pitMap != null) ? pitMap.get(lap.lapNumber) : null;

            Map<String, Object> enriched = new HashMap<>();
            enriched.put("session_key", lap.sessionKey);
            enriched.put("driver_number", lap.driverNumber);
            enriched.put("lap_number", lap.lapNumber);
            enriched.put("lap_duration", lap.lapDuration);
            enriched.put("date_start", lap.dateStart);
            enriched.put("is_pit_out_lap", lap.pitOutLap);
            enriched.put("pit_duration", pitDuration);
            enriched.put("had_pit_stop", pitDuration != null);

            out.collect(mapper.writeValueAsString(enriched));
        }

        @Override
        public void processElement2(String pitJson, Context ctx, Collector<String> out) throws Exception {
            PitStopEvent pit = mapper.readValue(pitJson, PitStopEvent.class);
            Map<Integer, Double> pitMap = pitStopState.value();
            if (pitMap == null) pitMap = new HashMap<>();
            pitMap.put(pit.lapNumber, pit.pitDuration);
            pitStopState.update(pitMap);
        }
    }

    private static String extractDriverNumber(String json, ObjectMapper mapper) {
        try {
            return mapper.readTree(json).path("driver_number").asText("0");
        } catch (Exception e) {
            return "0";
        }
    }
}
