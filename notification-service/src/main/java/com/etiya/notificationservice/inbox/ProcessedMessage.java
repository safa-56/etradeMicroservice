package com.etiya.notificationservice.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id", nullable = false, updatable = false)
    private String messageId;

    @Column(name = "consumer", nullable = false)
    private String consumer;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedMessage() {
    }

    public ProcessedMessage(String messageId, String consumer, String eventType, Instant processedAt) {
        this.messageId = messageId;
        this.consumer = consumer;
        this.eventType = eventType;
        this.processedAt = processedAt;
    }
}
