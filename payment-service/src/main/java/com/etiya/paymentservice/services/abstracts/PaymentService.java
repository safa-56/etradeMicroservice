package com.etiya.paymentservice.services.abstracts;

import com.etiya.paymentservice.events.OrderCreatedEvent;
import com.etiya.paymentservice.services.dtos.responses.GetAllPaymentsResponse;

import java.util.List;

public interface PaymentService {

    int completePaymentForOrder(OrderCreatedEvent event);

    List<GetAllPaymentsResponse> getAll();
}
