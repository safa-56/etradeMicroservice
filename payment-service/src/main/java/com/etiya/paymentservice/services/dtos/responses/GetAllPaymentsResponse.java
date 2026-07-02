package com.etiya.paymentservice.services.dtos.responses;

import java.math.BigDecimal;
import java.time.Instant;

public record GetAllPaymentsResponse(
        int id,
        int orderId,
        int customerId,
        BigDecimal amount,
        String status,
        Instant paidAt) {
}
