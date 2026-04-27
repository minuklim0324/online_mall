package com.mall.payment.dto;
import lombok.Data;
import java.math.BigDecimal;

@Data
public class PaymentResponse {
    private Long orderId;
    private BigDecimal amount;
}