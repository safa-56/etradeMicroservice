package com.etiya.orderservice.repositories;

import com.etiya.orderservice.entities.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA access to the {@code orders} table (PostgreSQL).
 *
 * <p>Backed by the same database as the outbox table, so order and outbox writes share one
 * transaction — the basis of the Transactional Outbox pattern.</p>
 */
@Repository
public interface OrderRepository extends JpaRepository<Order, Integer> {
}
