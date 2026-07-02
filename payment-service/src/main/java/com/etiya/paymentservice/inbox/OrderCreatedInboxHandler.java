package com.etiya.paymentservice.inbox;

import com.etiya.paymentservice.entities.Payment;
import com.etiya.paymentservice.events.OrderCreatedEvent;
import com.etiya.paymentservice.events.PaymentCompletedEvent;
import com.etiya.paymentservice.outbox.OutboxService;
import com.etiya.paymentservice.repositories.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class OrderCreatedInboxHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedInboxHandler.class);
    private static final String CONSUMER = "payment-service";
    private static final String EVENT_TYPE = "OrderCreated";
    private static final String PAYMENT_STATUS_COMPLETED = "COMPLETED";

    private final ProcessedMessageRepository processedRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxService outboxService;

    public OrderCreatedInboxHandler(ProcessedMessageRepository processedRepository,
                                    PaymentRepository paymentRepository,
                                    OutboxService outboxService) {
        this.processedRepository = processedRepository;
        this.paymentRepository = paymentRepository;
        this.outboxService = outboxService;
    }

    @Transactional
    public void handle(String messageId, OrderCreatedEvent event) {
        if (processedRepository.existsById(messageId)) {
            log.info("Duplicate OrderCreated skipped (messageId={}, orderId={})",
                    messageId, event.orderId());
            return;
        }

        Payment payment = paymentRepository.save(new Payment(
                event.orderId(),
                event.customerId(),
                event.totalPrice(),
                PAYMENT_STATUS_COMPLETED,
                Instant.now()));

        outboxService.record(
                "Payment",
                String.valueOf(payment.getId()),
                "PaymentCompleted",
                new PaymentCompletedEvent(
                        payment.getId(),
                        payment.getOrderId(),
                        payment.getCustomerId(),
                        payment.getAmount(),
                        payment.getStatus(),
                        payment.getPaidAt()));

        processedRepository.save(new ProcessedMessage(
                messageId,
                CONSUMER,
                EVENT_TYPE,
                Instant.now()));

        log.info("OrderCreated paid (messageId={}, orderId={}, paymentId={})",
                messageId, event.orderId(), payment.getId());
    }
}
