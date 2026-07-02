package com.etiya.paymentservice.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
