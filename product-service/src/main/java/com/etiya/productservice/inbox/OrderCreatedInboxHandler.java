package com.etiya.productservice.inbox;

import com.etiya.productservice.entities.Product;
import com.etiya.productservice.events.OrderCreatedEvent;
import com.etiya.productservice.repositories.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

/**
 * Applies the OrderCreated side effect exactly once, using the Inbox pattern.
 *
 * <p>Delivery is at-least-once, so the same event may arrive more than once. Everything below runs
 * in ONE transaction:</p>
 * <ol>
 *   <li>If {@code messageId} is already in {@link ProcessedMessageRepository}, skip — it was handled.</li>
 *   <li>Otherwise apply the business side effect (decrement the product's stock) and insert the
 *       inbox row.</li>
 * </ol>
 *
 * <p>The pre-check eliminates most duplicates cheaply. The real guarantee is the {@code message_id}
 * primary key: if two deliveries race past the check, the second {@code save} violates the
 * constraint, the whole transaction rolls back (so stock is NOT decremented twice), the Kafka offset
 * is not committed, and on the ensuing redelivery the pre-check finds the row and skips. Net effect:
 * the side effect is applied exactly once.</p>
 */
@Service
public class OrderCreatedInboxHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedInboxHandler.class);
    private static final String CONSUMER = "product-service";
    private static final String EVENT_TYPE = "OrderCreated";

    private final ProcessedMessageRepository processedRepository;
    private final ProductRepository productRepository;

    public OrderCreatedInboxHandler(ProcessedMessageRepository processedRepository,
                                    ProductRepository productRepository) {
        this.processedRepository = processedRepository;
        this.productRepository = productRepository;
    }

    /**
     * Asıl işin yapıldığı fonksiyondur. Önce duplicate kontrolü yapılır.
     * Eğer daha önceden böyle bir kayıt varsa gelen event tekrar işlenmez ve stok düşülmez.
     * Eğer daha önceden yoksa stok düşme işlemi gerçekleşir.
     * Daha sonra bu event işlendiği için inbox tablosuna kayıt işlemi yapılır.
     */
    @Transactional
    public void handle(String messageId, OrderCreatedEvent event) {
        if (processedRepository.existsById(messageId)) {
            log.info("Duplicate OrderCreated skipped (messageId={}, orderId={})",
                    messageId, event.orderId());
            return;
        }

        applyStockDecrement(event);

        processedRepository.save(
                new ProcessedMessage(messageId, CONSUMER, EVENT_TYPE, Instant.now()));

        log.info("OrderCreated processed (messageId={}, orderId={}, productId={}, quantity={})",
                messageId, event.orderId(), event.productId(), event.quantity());
    }

    /** Business side effect: reduce the ordered product's stock by the ordered quantity. */
    private void applyStockDecrement(OrderCreatedEvent event) {
        Optional<Product> found = productRepository.findById(event.productId());
        if (found.isEmpty()) {
            // Event still counts as processed (recorded by the caller) so it is not retried forever.
            log.warn("Product {} not found; recording OrderCreated as processed without stock change",
                    event.productId());
            return;
        }
        Product product = found.get();
        int previousStock = product.getStock();
        int newStock = previousStock - event.quantity();
        if (newStock < 0) {
            log.warn("Stock for product {} would go negative ({} - {}); clamping to 0",
                    product.getId(), previousStock, event.quantity());
            newStock = 0;
        }
        product.setStock(newStock);
        productRepository.save(product);
        log.info("Stock updated: productId={} {} -> {}", product.getId(), previousStock, newStock);
    }
}
