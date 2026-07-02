package com.etiya.paymentservice.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    public OutboxEvent record(String aggregateType, String aggregateId, String type, Object payload) {
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
