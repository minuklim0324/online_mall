// payment-service / StockResponse.java
package com.mall.payment.dto;
import lombok.Data;

@Data
public class StockResponse {
    private Long orderId;
    private String status; // FAIL일 때 환불 로직 가동
}