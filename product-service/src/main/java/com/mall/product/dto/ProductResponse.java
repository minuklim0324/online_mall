package com.mall.product.dto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder // 이 어노테이션이 있어야 .builder() 메서드를 쓸 수 있습니다.
@NoArgsConstructor
@AllArgsConstructor
public class ProductResponse {
    private Long productId;
    private String name;
    private BigDecimal price;
    private Integer stockCnt;
}