package com.mall.payment.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 결제 서비스의 Kafka 메시지 소비자(Consumer)
 * 주문 서비스로부터의 결제 요청을 처리하고 결과를 다시 통보합니다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // 토픽명 정의 (분석 엔진 및 테스트 코드 생성기에서 참조)
    public static final String TOPIC_PAYMENT_REQ = "payment-request";
    public static final String TOPIC_PAYMENT_RES = "payment-result-events";

    /**
     * 주문 서비스(Order Service)로부터 결제 요청 메시지를 수신합니다.
     * @param message JSON 형태의 결제 요청 문자열
     */
    //@KafkaListener(topics = TOPIC_PAYMENT_REQ, groupId = "pay-group")
    public void consume(String message) {
        log.info("[Kafka] 결제 요청 수신: {}", message);

        try {
            // 1. 수신된 JSON 문자열을 Map 구조로 역직렬화
            // TypeReference를 사용하여 제네릭 타입의 안정성을 확보하고 IDE 경고를 방지합니다.
            Map<String, Object> event = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            // 2. 비즈니스 로직 수행 (결제 처리)
            // 실제 환경에서는 PG사 연동이나 계좌 잔액 검증 로직이 호출됩니다.
            boolean isSuccess = processPayment(event);
            String resultStatus = isSuccess ? "SUCCESS" : "FAILED";

            // 3. 응답 데이터 구성
            // 주문 서비스에서 트랜잭션을 식별할 수 있도록 orderId를 포함합니다.
            Map<String, Object> resultEntry = new HashMap<>();
            resultEntry.put("orderId", event.get("orderId"));
            resultEntry.put("status", resultStatus);

            // 4. 결과 메시지 전송
            // 현재 Kafka 설정(StringSerializer)에 맞춰 객체를 JSON 문자열로 직렬화하여 전송합니다.
            // 이 과정이 누락되면 SerializationException이 발생할 수 있습니다.
            String jsonResult = objectMapper.writeValueAsString(resultEntry);
            // service.PaymentService.java 의 handlePaymentRequest 메소드 안에서 kafka 전송을 수행하고 있음
            //kafkaTemplate.send(TOPIC_PAYMENT_RES, jsonResult);

            log.info("[Kafka] 결제 결과 전송 완료: {}", jsonResult);

        } catch (JsonProcessingException e) {
            // JSON 파싱 또는 직렬화 실패 시 에러 로그 기록
            // ScenarioMiningInterceptor의 logErrorStep에 의해 자동으로 장애 시나리오로 수집됩니다.
            log.error("[Error] JSON 처리 중 오류 발생: {}", e.getMessage());
            kafkaTemplate.send(TOPIC_PAYMENT_RES, "{\"status\":\"FAILED\", \"reason\":\"JSON_ERROR\"}");
        } catch (Exception e) {
            log.error("[Error] 결제 처리 중 일반 오류 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 실제 결제 처리 로직 (시뮬레이션)
     * @param event 결제 요청 정보
     * @return 결제 성공 여부
     */
    private boolean processPayment(Map<String, Object> event) {
        // TODO: 실제 결제 게이트웨이(PG) 연동 로직 구현 필요
        // 현재는 시나리오 수집을 위해 항상 성공(true)을 반환하도록 설정됨
        return true;
    }
}