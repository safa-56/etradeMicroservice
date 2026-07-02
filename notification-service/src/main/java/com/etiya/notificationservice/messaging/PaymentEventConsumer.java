package com.etiya.notificationservice.messaging;

import com.etiya.notificationservice.events.PaymentCompletedEvent;
import com.etiya.notificationservice.inbox.PaymentCompletedInboxHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

@Configuration
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);
    private static final String DEBEZIUM_ID_HEADER = "eventId";
    private static final String EVENT_TYPE_FALLBACK_PREFIX = "PaymentCompleted:";

    @Bean
    public Consumer<Message<PaymentCompletedEvent>> paymentCompleted(PaymentCompletedInboxHandler handler) {
        return message -> {
            PaymentCompletedEvent event = message.getPayload();
            String messageId = resolveMessageId(message, event);
            handler.handle(messageId, event);
        };
    }

    private String resolveMessageId(Message<?> message, PaymentCompletedEvent event) {
        Object header = message.getHeaders().get(DEBEZIUM_ID_HEADER);
        String messageId = null;
        if (header instanceof byte[] bytes) {
            messageId = new String(bytes, StandardCharsets.UTF_8);
        } else if (header != null) {
            messageId = header.toString();
        }
        if (messageId == null || messageId.isBlank()) {
            messageId = EVENT_TYPE_FALLBACK_PREFIX + event.paymentId();
            log.warn("Debezium '{}' header missing; falling back to business key '{}'",
                    DEBEZIUM_ID_HEADER, messageId);
        }
        return messageId;
    }
}
