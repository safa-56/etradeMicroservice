package com.etiya.productservice.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Inbox / deduplication record: one row per event already processed by this service.
 *
 * <p>The {@code messageId} is the primary key, so its UNIQUE constraint — not an application-level
 * "have I seen this?" check — is the real idempotency guarantee. It is written in the SAME
 * transaction as the business side effect (stock decrement), so either both are committed or
 * neither is. On a redelivery the existing row is detected and the side effect is skipped.</p>
 *
 * <p>{@code messageId} comes from Debezium's Outbox Event Router {@code id} header (the outbox row's
 * UUID), which is stable across redeliveries. Since that id is globally unique the key is
 * {@code messageId} alone; if this table were shared by multiple consumers a composite
 * ({@code consumer}, {@code messageId}) key would be used instead.</p>
 */
// inbox class
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id", nullable = false, updatable = false)
    private String messageId;

    /** Logical consumer that processed the event; kept for diagnostics / future sharing. */
    @Column(name = "consumer", nullable = false)
    private String consumer;

    @Column(name = "event_type")
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    /** Required by JPA. */
    protected ProcessedMessage() {
    }

    public ProcessedMessage(String messageId, String consumer, String eventType, Instant processedAt) {
        this.messageId = messageId;
        this.consumer = consumer;
        this.eventType = eventType;
        this.processedAt = processedAt;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getConsumer() {
        return consumer;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
