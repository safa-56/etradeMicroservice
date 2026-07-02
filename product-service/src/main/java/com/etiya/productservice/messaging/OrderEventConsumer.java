package com.etiya.productservice.messaging;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import com.etiya.productservice.events.OrderCreatedEvent;
import com.etiya.productservice.inbox.OrderCreatedInboxHandler;

/**
 * Spring Cloud Stream consumer. The bean name {@code orderCreated} is referenced by
 * {@code spring.cloud.function.definition} and bound to the input binding
 * {@code orderCreated-in-0} (Kafka topic "order-created") in application.yml.
 *
 * <p>The message is consumed as a {@link Message} (not just the payload) so the idempotency key can
 * be read from Debezium's {@code id} header. Actual processing — with the Inbox de-duplication —
 * is delegated to {@link OrderCreatedInboxHandler}.</p>
 */
@Configuration
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    /**
     * Header carrying the outbox row's id (UUID) — stable across redeliveries, so it is the correct
     * de-duplication key. Deliberately NOT "id": Spring Messaging reserves the "id" (and "timestamp")
     * header names and overwrites them with a framework-generated value that changes on every
     * delivery, which would defeat de-duplication. Debezium emits this via
     * {@code transforms.outbox.table.fields.additional.placement: "id:header:eventId"}.
     */
    private static final String DEBEZIUM_ID_HEADER = "eventId";
    private static final String EVENT_TYPE_FALLBACK_PREFIX = "OrderCreated:";

    /**
     * Spring Cloud stream kullandığımız için @KafkaListener yok. Onun yerine aşağıdaki bean var.
     *  spring:
     *   cloud:
     *     function:
     *       definition: orderCreated
     *     stream:
     *       bindings:
     *         orderCreated-in-0:
     *           destination: order-created
     *           group: product-service
     * application.yml'daki yukarıdaki ayar ile kafka topic'e bağlanır.
     * Yani order-created topic'ine mesaj geldiğinde spring bu mesajı aşağıdaki fonksiyona verir.
    */
    @Bean
    public Consumer<Message<OrderCreatedEvent>> orderCreated(OrderCreatedInboxHandler handler) {
        return message -> {
            OrderCreatedEvent event = message.getPayload();
            String messageId = resolveMessageId(message, event);
            handler.handle(messageId, event);
        };
    }

    /**
     * Resolves the idempotency key from the Debezium {@code id} header. The Kafka header value can
     * arrive as a String or as raw {@code byte[]}, so both are handled. If the header is missing for
     * any reason, we fall back to a deterministic business key ({@code OrderCreated:<orderId>}) so
     * de-duplication still works — OrderCreated is emitted once per order.
     */
    private String resolveMessageId(Message<?> message, OrderCreatedEvent event) {
        Object header = message.getHeaders().get(DEBEZIUM_ID_HEADER);
        String messageId = null;
        if (header instanceof byte[] bytes) {
            messageId = new String(bytes, StandardCharsets.UTF_8);
        } else if (header != null) {
            messageId = header.toString();
        }
        if (messageId == null || messageId.isBlank()) {
            messageId = EVENT_TYPE_FALLBACK_PREFIX + event.orderId();
            log.warn("Debezium '{}' header missing; falling back to business key '{}'",
                    DEBEZIUM_ID_HEADER, messageId);
        }
        return messageId;
    }


}
