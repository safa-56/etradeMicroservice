package com.etiya.paymentservice.services.concretes;

import com.etiya.paymentservice.entities.Payment;
import com.etiya.paymentservice.events.OrderCreatedEvent;
import com.etiya.paymentservice.events.PaymentCompletedEvent;
import com.etiya.paymentservice.outbox.OutboxService;
import com.etiya.paymentservice.repositories.PaymentRepository;
import com.etiya.paymentservice.services.abstracts.PaymentService;
import com.etiya.paymentservice.services.dtos.responses.GetAllPaymentsResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class PaymentManager implements PaymentService {

    private static final String PAYMENT_STATUS_COMPLETED = "COMPLETED";

    private final PaymentRepository paymentRepository;
    private final OutboxService outboxService;

    public PaymentManager(PaymentRepository paymentRepository, OutboxService outboxService) {
        this.paymentRepository = paymentRepository;
        this.outboxService = outboxService;
    }

    @Override
    @Transactional
    public int completePaymentForOrder(OrderCreatedEvent event) {
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

        return payment.getId();
    }

    @Override
    public List<GetAllPaymentsResponse> getAll() {
        return paymentRepository.findAll().stream()
                .map(payment -> new GetAllPaymentsResponse(
                        payment.getId(),
                        payment.getOrderId(),
                        payment.getCustomerId(),
                        payment.getAmount(),
                        payment.getStatus(),
                        payment.getPaidAt()))
                .toList();
    }
}
