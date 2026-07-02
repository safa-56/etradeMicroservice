package com.etiya.notificationservice.inbox;

import com.etiya.notificationservice.entities.Notification;
import com.etiya.notificationservice.events.PaymentCompletedEvent;
import com.etiya.notificationservice.repositories.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class PaymentCompletedInboxHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedInboxHandler.class);
    private static final String CONSUMER = "notification-service";
    private static final String EVENT_TYPE = "PaymentCompleted";

    private final ProcessedMessageRepository processedRepository;
    private final NotificationRepository notificationRepository;

    public PaymentCompletedInboxHandler(ProcessedMessageRepository processedRepository,
                                        NotificationRepository notificationRepository) {
        this.processedRepository = processedRepository;
        this.notificationRepository = notificationRepository;
    }

    @Transactional
    public void handle(String messageId, PaymentCompletedEvent event) {
        if (processedRepository.existsById(messageId)) {
            log.info("Duplicate PaymentCompleted skipped (messageId={}, paymentId={})",
                    messageId, event.paymentId());
            return;
        }

        notificationRepository.save(new Notification(
                event.customerId(),
                event.orderId(),
                "PAYMENT_COMPLETED",
                "Payment completed for order " + event.orderId(),
                "SENT",
                Instant.now()));

        processedRepository.save(new ProcessedMessage(
                messageId,
                CONSUMER,
                EVENT_TYPE,
                Instant.now()));

        log.info("PaymentCompleted notification recorded (messageId={}, paymentId={}, orderId={})",
                messageId, event.paymentId(), event.orderId());
    }
}
