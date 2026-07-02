package com.etiya.orderservice.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Spring Data JPA access to the outbox table.
 *
 * <p>Only inserts are performed here; Debezium reads the resulting WAL entries and publishes them
 * to Kafka, so no polling/query-by-status method is needed anymore.</p>
 */
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
}
