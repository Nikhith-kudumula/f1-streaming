package com.f1streaming.producer;

import com.f1streaming.producer.config.ProducerConfig;
import com.f1streaming.producer.service.KafkaEventPublisher;
import com.f1streaming.producer.service.OpenF1Client;
import com.f1streaming.producer.service.ReplayOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerApp {

    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    public static void main(String[] args) throws InterruptedException {
        ProducerConfig config = ProducerConfig.fromEnv();

        log.info("Starting F1 Producer — session={} bootstrap={}",
                config.getSessionKey(), config.getBootstrapServers());

        try (KafkaEventPublisher publisher = new KafkaEventPublisher(config.getBootstrapServers())) {
            OpenF1Client client = new OpenF1Client(config.getSessionKey());
            ReplayOrchestrator orchestrator = new ReplayOrchestrator(client, publisher, config.getReplaySpeed());
            orchestrator.run();
        }

        log.info("Producer finished.");
    }
}
