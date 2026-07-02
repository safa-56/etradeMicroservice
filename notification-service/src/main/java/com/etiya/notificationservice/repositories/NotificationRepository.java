package com.etiya.notificationservice.repositories;

import com.etiya.notificationservice.entities.Notification;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationRepository extends JpaRepository<Notification, Integer> {
}
