package com.etiya.paymentservice.controllers;

import com.etiya.paymentservice.entities.Payment;
import com.etiya.paymentservice.repositories.PaymentRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
public class PaymentsController {

    private final PaymentRepository paymentRepository;

    public PaymentsController(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @GetMapping
    public List<Payment> getAll() {
        return paymentRepository.findAll();
    }
}
