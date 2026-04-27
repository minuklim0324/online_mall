// order-service / PaymentResponse.java
package com.mall.order.dto;
import lombok.Data;

@Data
public class PaymentResponse {
    private Long orderId;
    private String status; // SUCCESS, FAIL
}