package com.etiya.productservice.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA access to the inbox table. {@code existsById} is used as a fast pre-check;
 * the primary-key constraint on {@code message_id} is the actual duplicate guard.
 */
public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
