package com.etiya.paymentservice.repositories;

import com.etiya.paymentservice.entities.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Integer> {
}
