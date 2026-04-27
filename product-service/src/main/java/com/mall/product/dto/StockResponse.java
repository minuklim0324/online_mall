package com.mall.product.dto; // 또는 공통 패키지
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockResponse {
    private Long orderId;      // 어떤 주문에 대한 재고 처리인지
    private String status;     // SUCCESS 또는 FAIL
    private String reason;     // 실패 시 사유 (예: "재고 부족")
}