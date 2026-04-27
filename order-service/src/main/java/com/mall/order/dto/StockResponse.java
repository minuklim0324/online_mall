// order-service / StockResponse.java
package com.mall.order.dto;
import lombok.Data;

@Data
public class StockResponse {
    private Long orderId;
    private String status; // SUCCESS, FAIL
    private String reason;
}