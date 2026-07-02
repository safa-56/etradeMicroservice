package com.etiya.paymentservice.events;

import java.math.BigDecimal;

public record OrderCreatedEvent(
        int orderId,
        int customerId,
        int productId,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal totalPrice,
        String address) {
}
