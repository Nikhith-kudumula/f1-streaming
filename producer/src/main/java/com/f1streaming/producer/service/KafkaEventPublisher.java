package com.f1streaming.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaEventPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;

    public KafkaEventPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1");
        props.put("retries", "3");
        props.put("linger.ms", "5");
        this.producer = new KafkaProducer<>(props);
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public <T> void publish(String topic, String key, T event) {
        try {
            String json = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, key, json), (metadata, ex) -> {
                if (ex != null) {
                    log.error("Failed to send to topic {}", topic, ex);
                } else {
                    log.debug("Sent to {}[{}] offset={}", topic, metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            log.error("Serialization failed for event on topic {}", topic, e);
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}
