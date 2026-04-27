package com.mall.product.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Entity
@Table(name = "products")
@Getter
@NoArgsConstructor // JPA 엔티티는 기본 생성자가 필수입니다.
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long productId;

    private String name;

    // 기존 서비스에서 getDesc()를 호출하므로 필드명을 맞추거나 별도의 getter가 필요합니다.
    @Column(name = "description") // DB 컬럼명에 맞춰 조정하세요.
    private String description;

    private BigDecimal price;

    private Integer stockCnt;

    /**
     * 재고 차감 로직 보완
     * 1. 재고 부족 시 RuntimeException을 던져 트랜잭션 롤백을 유도합니다.
     * 2. NullPointerException 방지를 위해 null 체크를 포함합니다.
     */
    public void decrease(int qty) {
        if (this.stockCnt == null || this.stockCnt < qty) {
            throw new RuntimeException("재고가 부족합니다. (현재 재고: " + (this.stockCnt == null ? 0 : this.stockCnt) + ")");
        }
        this.stockCnt -= qty;
    }
}