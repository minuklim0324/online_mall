package com.mall.order.controller;
import com.mall.order.dto.OrderRequest;
import com.mall.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController @RequestMapping("/api/orders") @RequiredArgsConstructor
public class OrderController {
    private final OrderService orderService;
    @PostMapping
    public void placeOrder(@RequestBody OrderRequest req) {
        orderService.process(req);
    }
}