package com.mall.order.dto; // 각 서비스 패키지에 맞게 수정 (payment.dto, product.dto)

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
    private Integer qty; // 변수명을 orderCnt -> qyt 로 변경(3/30)
    private BigDecimal amount; // [추가] 결제 금액 (Payment 서비스에서 필수)
    private String status;     // [추가] 주문 상태 전달용
}