package com.mall.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mall.order.repository.OrderMapper;
import com.mall.order.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord; // ConsumerRecord 추가
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderMapper orderMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    public static final String TOPIC_PAYMENT_REQ = "payment-request";
    public static final String TOPIC_STOCK_RES = "stock-result-events";
    public static final String TOPIC_PAYMENT_RES = "payment-result-events";

    @Value("${services.product-service.url}")
    private String productServiceUrl;

    public void process(OrderRequest req) {
        log.info(">>>> [데이터 체크] productId: {}, qty: {}, amount: {}",
                req.getProductId(), req.getQty(), req.getAmount());

        String url = productServiceUrl + "/api/products/" + req.getProductId();

        // 🌟 [전파 1] HTTP 호출 시 scenarioId 전파 (헤더 추가)
        HttpHeaders headers = new HttpHeaders();
        String currentSid = getCurrentScenarioId();
        if (currentSid != null) {
            headers.set("X-Scenario-Id", currentSid);
        }
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        // restTemplate.getForObject 대신 exchange를 사용하여 헤더 포함 호출
        ProductResponse product = restTemplate.exchange(url, HttpMethod.GET, entity, ProductResponse.class).getBody();

        if (product != null && product.getStockCnt() < req.getQty()) {
            throw new RuntimeException("재고 부족 (현재 수량: " + product.getStockCnt() + ")");
        }

        req.setStatus("PENDING");
        int result = orderMapper.insertOrder(req);

        if (result == 0) {
            log.error("!!!! DB 저장에 실패했습니다 !!!!");
            return;
        }

        try {
            String jsonMessage = objectMapper.writeValueAsString(req);
            // 🌟 [전파 2] Kafka 전송은 인터셉터(injectIdToKafkaHeader)가 이미 처리 중이므로 기존대로 유지
            kafkaTemplate.send(TOPIC_PAYMENT_REQ, jsonMessage);
            log.info("결제 서비스로 토픽 전송 완료: {}", TOPIC_PAYMENT_REQ);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("메시지 변환 실패", e);
        }
    }

    // 🌟 [수신 1] ConsumerRecord를 인자로 받아 인터셉터가 헤더를 읽을 수 있게 보완
    @KafkaListener(topics = TOPIC_STOCK_RES, groupId = "order-group")
    @Transactional
    public void handleStockResult(ConsumerRecord<String, String> record) {
        String message = record.value();
        try {
            StockResponse res = objectMapper.readValue(message, StockResponse.class);
            log.info("재고 처리 결과 수신: 주문번호={}, 상태={}", res.getOrderId(), res.getStatus());

            String currentStatus = orderMapper.findStatusById(res.getOrderId());
            if (currentStatus == null || "COMPLETED".equals(currentStatus) || "CANCELLED".equals(currentStatus)) {
                return;
            }

            orderMapper.updateStatus(res.getOrderId(), "SUCCESS".equals(res.getStatus()) ? "COMPLETED" : "CANCELLED");
        } catch (JsonProcessingException e) {
            log.error("재고 결과 파싱 에러: {}", e.getMessage());
        }
    }

    // 🌟 [수신 2] ConsumerRecord를 인자로 받아 인터셉터가 헤더를 읽을 수 있게 보완
    @KafkaListener(topics = TOPIC_PAYMENT_RES, groupId = "order-group")
    @Transactional
    public void handlePaymentFail(ConsumerRecord<String, String> record) {
        String message = record.value();
        try {
            log.info(">>>> [주문 서비스] 결제 결과 수신: {}", message);
            PaymentResponse res = objectMapper.readValue(message, PaymentResponse.class);

            if ("FAIL".equals(res.getStatus())) {
                String currentStatus = orderMapper.findStatusById(res.getOrderId());
                if (!"CANCELLED".equals(currentStatus)) {
                    orderMapper.updateStatus(res.getOrderId(), "CANCELLED");
                }
            }
        } catch (JsonProcessingException e) {
            log.error("결제 결과 파싱 에러: {}", e.getMessage());
        }
    }

    /**
     * 현재 쓰레드의 Request에서 scenarioId 추출 (테스트용)
     */
    private String getCurrentScenarioId() {
        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null) {
                return attrs.getRequest().getHeader("X-Scenario-Id");
            }
        } catch (Exception ignored) {}
        return null;
    }
}