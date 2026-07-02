package com.etiya.notificationservice.controllers;

import com.etiya.notificationservice.entities.Notification;
import com.etiya.notificationservice.repositories.NotificationRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
public class NotificationsController {

    private final NotificationRepository notificationRepository;

    public NotificationsController(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    @GetMapping
    public List<Notification> getAll() {
        return notificationRepository.findAll();
    }
}
