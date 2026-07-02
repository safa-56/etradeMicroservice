package com.etiya.productservice.repositories;

import com.etiya.productservice.entities.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA access to the {@code products} table (PostgreSQL).
 */
@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {
}
