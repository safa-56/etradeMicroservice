package com.etiya.paymentservice.inbox;

import com.etiya.paymentservice.events.OrderCreatedEvent;
import com.etiya.paymentservice.services.abstracts.PaymentService;
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

    private final ProcessedMessageRepository processedRepository;
    private final PaymentService paymentService;

    public OrderCreatedInboxHandler(ProcessedMessageRepository processedRepository,
                                    PaymentService paymentService) {
        this.processedRepository = processedRepository;
        this.paymentService = paymentService;
    }

    @Transactional
    public void handle(String messageId, OrderCreatedEvent event) {
        if (processedRepository.existsById(messageId)) {
            log.info("Duplicate OrderCreated skipped (messageId={}, orderId={})",
                    messageId, event.orderId());
            return;
        }

        int paymentId = paymentService.completePaymentForOrder(event);

        processedRepository.save(new ProcessedMessage(
                messageId,
                CONSUMER,
                EVENT_TYPE,
                Instant.now()));

        log.info("OrderCreated paid (messageId={}, orderId={}, paymentId={})",
                messageId, event.orderId(), paymentId);
    }
}
