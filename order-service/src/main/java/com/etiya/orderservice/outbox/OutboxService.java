package com.etiya.orderservice.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Writes events into the outbox table. The business layer calls {@link #record} instead of
 * publishing to Kafka directly, so the message is durably queued in the same DB transaction as the
 * business write. Debezium (CDC) later streams the insert from Postgres' WAL to Kafka.
 */
@Service
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Serializes {@code payload} to JSON and inserts one outbox row.
     *
     * @param aggregateType domain aggregate, e.g. {@code "Order"} (used by Debezium for routing)
     * @param aggregateId   aggregate instance id, e.g. the order id (becomes the Kafka key)
     * @param type          logical event name, e.g. {@code "OrderCreated"}
     * @param payload       event body, serialized to JSON and emitted as the Kafka message value
     */
    public OutboxEvent record(String aggregateType, String aggregateId, String type,
                              Object payload) {
        OutboxEvent event = new OutboxEvent(
                aggregateType,
                aggregateId,
                type,
                serialize(payload),
                Instant.now());
        return outboxRepository.save(event);
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload: " + payload, e);
        }
    }
}
