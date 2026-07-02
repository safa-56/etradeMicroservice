package com.etiya.notificationservice.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
