package com.etiya.notificationservice.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "notifications")
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(nullable = false)
    private int customerId;

    @Column(nullable = false)
    private int orderId;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private String message;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private Instant createdAt;

    protected Notification() {
    }

    public Notification(int customerId, int orderId, String type,
                        String message, String status, Instant createdAt) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.type = type;
        this.message = message;
        this.status = status;
        this.createdAt = createdAt;
    }

    public int getId() {
        return id;
    }

    public int getCustomerId() {
        return customerId;
    }

    public int getOrderId() {
        return orderId;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
