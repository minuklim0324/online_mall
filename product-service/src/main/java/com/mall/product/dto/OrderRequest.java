package com.mall.product.dto; // 패키지만 product로 변경

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest { 
    private Long orderId; 
    private String userId; 
    private Long productId; 
    private Integer qty;
    private BigDecimal amount; // [추가] 결제 금액 (Payment 서비스에서 필수)
    private String status;     // [추가] 주문 상태 전달용
}