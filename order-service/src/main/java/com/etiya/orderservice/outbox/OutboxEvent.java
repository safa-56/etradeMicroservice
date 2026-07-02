package com.etiya.orderservice.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * A row in the Transactional Outbox table (PostgreSQL), captured by Debezium CDC.
 *
 * <p>The business layer inserts one row here inside the same transaction as the order write. It is
 * <strong>insert-only</strong>: there is no {@code status}/{@code retryCount} bookkeeping anymore,
 * because publishing to Kafka is no longer done by a polling relay but by Debezium's
 * {@code io.debezium.transforms.outbox.EventRouter}, which reads the WAL and forwards each insert.</p>
 *
 * <p>Column names deliberately match Debezium's Outbox Event Router default convention
 * ({@code id}, {@code aggregatetype}, {@code aggregateid}, {@code type}, {@code payload}) so the
 * connector config stays minimal:
 * <ul>
 *   <li>{@code aggregatetype} — routes the message to a topic (mapped to {@code order-created}).</li>
 *   <li>{@code aggregateid}   — becomes the Kafka message key (ordering per aggregate).</li>
 *   <li>{@code type}          — logical event name, e.g. {@code OrderCreated}.</li>
 *   <li>{@code payload}       — JSON event body, emitted as the Kafka message value.</li>
 * </ul>
 * </p>
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    /** Event id; also emitted by Debezium as a header for consumer-side idempotency. */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /** Domain aggregate the event belongs to, e.g. {@code "Order"}. Used for topic routing. */
    @Column(name = "aggregatetype", nullable = false)
    private String aggregateType;

    /** Identifier of the aggregate instance, e.g. the order id. Becomes the Kafka message key. */
    @Column(name = "aggregateid", nullable = false)
    private String aggregateId;

    /** Logical event name, e.g. {@code "OrderCreated"}. */
    @Column(name = "type", nullable = false)
    private String type;

    /** Serialized (JSON) event body, emitted to Kafka as-is. */
    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    /** When the event was recorded; diagnostics only, not required by Debezium. */
    @Column(name = "createdat", nullable = false)
    private Instant createdAt;

    /** Required by JPA. */
    protected OutboxEvent() {
    }

    public OutboxEvent(String aggregateType, String aggregateId, String type,
                       String payload, Instant createdAt) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.type = type;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
