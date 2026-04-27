package com.mall.order.entity;
import lombok.Data;
import jakarta.persistence.*; // JPA 사용 시

@Data
@Entity
@Table(name = "orders")
public class Order {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String productId;
    private Integer qty;
    private Long totalPrice;
    private String status;
}