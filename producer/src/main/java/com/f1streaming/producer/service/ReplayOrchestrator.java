package com.f1streaming.producer.service;

import com.f1streaming.producer.config.ProducerConfig;
import com.f1streaming.producer.model.DriverPosition;
import com.f1streaming.producer.model.LapTime;
import com.f1streaming.producer.model.PitStop;
import com.f1streaming.producer.model.RaceControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;

/**
 * Replays a historical OpenF1 session by emitting events in timestamp order
 * with wall-clock delays scaled by replaySpeed.
 */
public class ReplayOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ReplayOrchestrator.class);

    private final OpenF1Client client;
    private final KafkaEventPublisher publisher;
    private final double replaySpeed;

    public ReplayOrchestrator(OpenF1Client client, KafkaEventPublisher publisher, double replaySpeed) {
        this.client = client;
        this.publisher = publisher;
        this.replaySpeed = replaySpeed;
    }

    public void run() throws InterruptedException {
        log.info("Fetching session data from OpenF1...");

        List<DriverPosition> positions   = client.fetchPositions();
        List<LapTime>        laps        = client.fetchLaps();
        List<PitStop>        pitStops    = client.fetchPitStops();
        List<RaceControlMessage> rcMsgs  = client.fetchRaceControl();

        log.info("Loaded: {} positions, {} laps, {} pit stops, {} race control messages",
                positions.size(), laps.size(), pitStops.size(), rcMsgs.size());

        // Build a unified timeline of typed events sorted by their timestamp
        record TimedEvent(Instant time, String topic, String key, Object payload) {}

        java.util.List<TimedEvent> timeline = new java.util.ArrayList<>();

        positions.forEach(p -> timeline.add(new TimedEvent(
                parseTime(p.getDate()), ProducerConfig.TOPIC_POSITIONS,
                String.valueOf(p.getDriverNumber()), p)));

        laps.forEach(l -> timeline.add(new TimedEvent(
                parseTime(l.getDateStart()), ProducerConfig.TOPIC_LAPS,
                String.valueOf(l.getDriverNumber()), l)));

        pitStops.forEach(p -> timeline.add(new TimedEvent(
                parseTime(p.getDate()), ProducerConfig.TOPIC_PIT_STOPS,
                String.valueOf(p.getDriverNumber()), p)));

        rcMsgs.forEach(r -> timeline.add(new TimedEvent(
                parseTime(r.getDate()), ProducerConfig.TOPIC_RACE_CONTROL,
                "rc", r)));

        timeline.sort(Comparator.comparing(TimedEvent::time));

        if (timeline.isEmpty()) {
            log.warn("No events to replay — check session key or API availability.");
            return;
        }

        Instant sessionStart = timeline.get(0).time();
        Instant replayStart  = Instant.now();

        log.info("Starting replay of {} events at {}x speed", timeline.size(), replaySpeed);

        for (TimedEvent event : timeline) {
            long sessionOffsetMs = event.time().toEpochMilli() - sessionStart.toEpochMilli();
            long targetMs = (long) (sessionOffsetMs / replaySpeed);
            long waitMs   = targetMs - (Instant.now().toEpochMilli() - replayStart.toEpochMilli());

            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }

            publisher.publish(event.topic(), event.key(), event.payload());
        }

        publisher.flush();
        log.info("Replay complete.");
    }

    private Instant parseTime(String dateStr) {
        if (dateStr == null) return Instant.EPOCH;
        try {
            return OffsetDateTime.parse(dateStr).toInstant();
        } catch (Exception e) {
            return Instant.EPOCH;
        }
    }
}
