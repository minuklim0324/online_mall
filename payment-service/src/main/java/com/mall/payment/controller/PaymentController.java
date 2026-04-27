package com.mall.payment.controller;

import com.mall.payment.repository.PaymentMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentMapper paymentMapper;

    @GetMapping("/status/{orderId}")
    public String getPaymentStatus(@PathVariable Long orderId) {
        // Mapper를 직접 부르거나 Service를 거쳐 상태 반환
        return paymentMapper.findStatusByOrderId(orderId);
    }
}