package com.etiya.notificationservice.events;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
        int paymentId,
        int orderId,
        int customerId,
        BigDecimal amount,
        String status,
        Instant paidAt) {
}
