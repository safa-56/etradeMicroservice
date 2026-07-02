package com.etiya.paymentservice.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "aggregatetype", nullable = false)
    private String aggregateType;

    @Column(name = "aggregateid", nullable = false)
    private String aggregateId;

    @Column(name = "type", nullable = false)
    private String type;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "createdat", nullable = false)
    private Instant createdAt;

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
}
