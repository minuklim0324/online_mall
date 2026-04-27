package com.mall.product.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mall.product.entity.Product;
import com.mall.product.repository.ProductRepository;
import com.mall.product.dto.ProductResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord; // ConsumerRecord 추가
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public static final String TOPIC_PAYMENT_RES = "payment-result-events";
    public static final String TOPIC_STOCK_RES = "stock-result-events";

    // --- [조회 메서드: 기존 유지] ---

    @Transactional(readOnly = true)
    public List<ProductResponse> getAllProducts() {
        return productRepository.findAll().stream()
                .map(this::convertToResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public ProductResponse getProduct(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("상품을 찾을 수 없습니다. ID: " + id));
        return convertToResponse(product);
    }

    private ProductResponse convertToResponse(Product p) {
        return new ProductResponse(
                p.getProductId(),
                p.getName(),
                p.getPrice(),
                p.getStockCnt()
        );
    }

    // --- [Kafka 메시지 처리 로직 보완] ---

    /**
     * 🌟 보완: ConsumerRecord를 인자로 받아 인터셉터가 헤더의 scenarioId를 읽을 수 있게 함
     */
    @Transactional
    @KafkaListener(topics = TOPIC_PAYMENT_RES, groupId = "product-group")
    public void handlePaymentResult(ConsumerRecord<String, String> record) {
        String message = record.value(); // 본문 추출

        if (message == null || !message.trim().startsWith("{")) {
            return;
        }

        try {
            Map<String, Object> req = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            if (req.get("productId") == null || req.get("orderId") == null) {
                log.error(">>>> [중단] 필수 데이터 누락: {}", req);
                return;
            }

            Long orderId = Long.valueOf(req.get("orderId").toString());
            Long productId = Long.valueOf(req.get("productId").toString());
            int qty = Integer.parseInt(req.getOrDefault("qty", 0).toString());

            processStockUpdate(orderId, productId, qty);

        } catch (Exception e) {
            log.error(">>>> 시스템 에러: {}", e.getMessage());
            // 인터셉터가 에러를 기록할 수 있도록 예외를 던지는 것이 좋습니다.
            throw new RuntimeException("재고 처리 중 시스템 에러", e);
        }
    }

    private void processStockUpdate(Long orderId, Long productId, int qty) {
        String status = "SUCCESS";
        String reason = "";

        try {
            Product product = productRepository.findById(productId)
                    .orElseThrow(() -> new RuntimeException("상품 없음 ID: " + productId));

            product.decrease(qty); // 재고 차감 로직
            productRepository.save(product);
            log.info(">>>> 재고 차감 성공: 주문번호 {}", orderId);

        } catch (Exception e) {
            status = "FAIL";
            reason = e.getMessage();
            log.error(">>>> 재고 차감 실패: 주문번호 {}, 사유: {}", orderId, reason);
        }

        // 🌟 결과 전송 (현재 쓰레드에 맺힌 scenarioId가 Kafka 헤더에 자동 포함됨)
        sendStockResult(orderId, status, reason);
    }

    private void sendStockResult(Long orderId, String status, String reason) {
        try {
            Map<String, Object> res = Map.of(
                    "orderId", orderId,
                    "status", status,
                    "reason", (reason != null ? reason : "")
            );
            kafkaTemplate.send(TOPIC_STOCK_RES, objectMapper.writeValueAsString(res));
        } catch (Exception e) {
            log.error(">>>> 결과 전송 실패: {}", e.getMessage());
        }
    }
}