package com.mall.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mall.payment.repository.PaymentMapper;
import com.mall.payment.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord; // ConsumerRecord 추가
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentMapper paymentMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public static final String TOPIC_PAYMENT_REQ = "payment-request";
    public static final String TOPIC_PAYMENT_RES = "payment-result-events";
    public static final String TOPIC_STOCK_RES = "stock-result-events";

    /**
     * 1. 주문 서비스로부터 결제 요청 수신
     * 🌟 보완: ConsumerRecord를 사용하여 scenarioId 헤더 전파를 허용
     */
    @KafkaListener(topics = TOPIC_PAYMENT_REQ, groupId = "payment-group")
    @Transactional
    public void handlePaymentRequest(ConsumerRecord<String, String> record) {
        String message = record.value(); // 본문 추출
        try {
            log.info(">>>> [결제 서비스] 수신된 원본 메시지: {}", message);

            OrderRequest req = objectMapper.readValue(message, OrderRequest.class);
            log.info(">>>> [변환 완료] 주문번호: {}, 금액: {}", req.getOrderId(), req.getAmount());

            // 결제 상태 DB 저장
            paymentMapper.insertPayment(req.getOrderId(), req.getQty(), "PAID");

            // 🌟 결과 전송 (인터셉터가 현재 쓰레드의 ID를 다시 헤더에 심어 보냅니다)
            String jsonRes = objectMapper.writeValueAsString(req);
            kafkaTemplate.send(TOPIC_PAYMENT_RES, jsonRes);

            log.info(">>>> 결제 완료 이벤트 발행 완료: 주문번호 {}", req.getOrderId());

        } catch (JsonProcessingException e) {
            log.error(">>>> [에러] 메시지 변환 중 오류 발생: {}", e.getMessage());
            throw new RuntimeException("결제 처리 실패", e);
        }
    }

    /**
     * 2. 재고 서비스로부터 결과 수신 (보상 트랜잭션 처리)
     * 🌟 보완: ConsumerRecord를 사용하여 재고 서비스의 시나리오 흐름과 연결
     */
    @KafkaListener(topics = TOPIC_STOCK_RES, groupId = "payment-group")
    public void handleStockResult(ConsumerRecord<String, String> record) {
        String message = record.value();
        log.info(">>>> [재고 서비스 결과 수신]: {}", message);

        try {
            StockResponse response = objectMapper.readValue(message, StockResponse.class);
            log.info(">>>> [변환 성공] 주문번호: {}, 상태: {}", response.getOrderId(), response.getStatus());

            // 멱등성 및 보상 트랜잭션 로직 (필요 시 구현)
            // if ("FAIL".equals(response.getStatus())) { ... 환불 처리 ... }

        } catch (JsonProcessingException e) {
            log.error(">>>> [JSON 변환 실패]: {}", e.getMessage());
        }
    }
}