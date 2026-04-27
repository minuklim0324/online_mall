package com.mall.order.client;
import com.mall.order.dto.ProductResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "product-service", url = "${services.product-service.url}")
public interface ProductClient {
    @GetMapping("/api/products/{id}")
    ProductResponse getProductDetail(@PathVariable("id") Long id);
    //체크 3/5
}