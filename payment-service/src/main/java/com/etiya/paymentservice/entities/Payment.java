package com.etiya.paymentservice.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(nullable = false, unique = true)
    private int orderId;

    @Column(nullable = false)
    private int customerId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private Instant paidAt;

    protected Payment() {
    }

    public Payment(int orderId, int customerId, BigDecimal amount, String status, Instant paidAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.paidAt = paidAt;
    }

    public int getId() {
        return id;
    }

    public int getOrderId() {
        return orderId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getStatus() {
        return status;
    }

    public Instant getPaidAt() {
        return paidAt;
    }
}
