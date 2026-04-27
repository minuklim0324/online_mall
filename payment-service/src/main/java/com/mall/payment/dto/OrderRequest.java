// payment-service / OrderRequest.java
package com.mall.payment.dto;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class OrderRequest {
    private Long orderId;
    private String userId;
    private Long productId;
    private Integer qty;
    private BigDecimal amount; // 결제 금액
}