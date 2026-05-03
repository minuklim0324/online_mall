package com.mall.product.config.aop;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class ScenarioMiningInterceptor implements MethodInterceptor {

    // 기존: private final ObjectMapper objectMapper = new ObjectMapper();
    // 수정: 생성자나 초기화 블록에서 설정 추가
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    private static final String BASE_LOG_PATH = "C:/app/pilot-p/scenario/log/";
    private String serviceLogPath;
    private String currentServiceName;
    private static final ThreadLocal<Map<String, Object>> scenarioContext = new ThreadLocal<>();

    public void initLogPath(String serviceName) {
        this.currentServiceName = serviceName;
        this.serviceLogPath = BASE_LOG_PATH + serviceName + "/";
        File directory = new File(this.serviceLogPath);
        if (!directory.exists()) directory.mkdirs();
    }
    private Map<String, Object> getEntryPointInfo(Object[] args) {
        Map<String, Object> entryInfo = new LinkedHashMap<>();

        // 이 로그를 기록하는 현재 시스템 (Target)
        entryInfo.put("target_system", this.currentServiceName);

        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof org.apache.kafka.clients.consumer.ConsumerRecord) {
                    org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record = (org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>) arg;
                    entryInfo.put("entry_type", "KAFKA_TOPIC");
                    entryInfo.put("endpoint", record.topic());

                    // [수정] Kafka 헤더에서 실제 발행 시스템(Source) 추출
                    org.apache.kafka.common.header.Header sourceHeader = record.headers().lastHeader("X-Source-Service");
                    if (sourceHeader != null) {
                        entryInfo.put("caller_system", new String(sourceHeader.value()));
                    } else {
                        // 헤더가 없는 경우 토픽명으로 추정하는 룰셋 적용 가능
                        if (record.topic().contains("stock")) entryInfo.put("caller_system", "product-service");
                        else if (record.topic().contains("payment")) entryInfo.put("caller_system", "payment-service");
                        else entryInfo.put("caller_system", "UNKNOWN_KAFKA_SENDER");
                    }
                    return entryInfo;
                }
            }
        }

        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null) {
                HttpServletRequest request = attrs.getRequest();

                // 1. 누가 호출했는지 헤더에서 추출 (약속된 헤더가 있다면 사용)
                String caller = request.getHeader("X-Source-Service");
                if (caller == null) {
                    // 헤더가 없다면 User-Agent나 Referer 등을 통해 UI 유입인지 판단 가능
                    caller = (request.getHeader("User-Agent") != null) ? "FRONT_UI" : "UNKNOWN_CLIENT";
                }

                entryInfo.put("caller_system", caller); // [추가] 호출한 시스템 정보
                entryInfo.put("entry_type", "REST_API");
                entryInfo.put("endpoint", request.getRequestURI());
                entryInfo.put("http_method", request.getMethod());
                return entryInfo;
            }
        } catch (Exception ignored) {}

        // 1. Kafka 유입 확인 (ConsumerRecord 존재 여부)
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof org.apache.kafka.clients.consumer.ConsumerRecord) {
                    org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record = (org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>) arg;
                    entryInfo.put("entry_type", "KAFKA_TOPIC");
                    entryInfo.put("endpoint", record.topic());
                    return entryInfo;
                }
            }
        }

        // 2. HTTP API 유입 확인
        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null) {
                HttpServletRequest request = attrs.getRequest();
                entryInfo.put("entry_type", "REST_API");
                entryInfo.put("endpoint", request.getRequestURI());
                entryInfo.put("http_method", request.getMethod());
                return entryInfo;
            }
        } catch (Exception ignored) {}

        // 3. 기타 유입
        entryInfo.put("entry_type", "INTERNAL");
        entryInfo.put("endpoint", "unknown");

        return entryInfo;
    }

    @Override
    public Object invoke(@NonNull MethodInvocation invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        String className = (invocation.getThis() != null) ? invocation.getThis().getClass().getSimpleName() : "Unknown";
        String methodName = invocation.getMethod().getName();
        boolean isFirstEntry = false;
        // 1. 비즈니스 진입점 판단 및 컨텍스트 초기화
        if (scenarioContext.get() == null && isBusinessEntryPoint(className, methodName)) {
            String extractedId = findTraceIdFromArgs(args);
            Map<String, Object> context = new HashMap<>();
            // [수정] 유입 경로 정보 추출 및 추가
            Map<String, Object> entryInfo = getEntryPointInfo(args);
            context.put("entryInfo", entryInfo);

            context.put("scenarioId", (extractedId != null) ? extractedId : "TRX-" + System.currentTimeMillis());
            context.put("steps", new ArrayList<Map<String, Object>>());
            scenarioContext.set(context);
            isFirstEntry = true;
        }
        // 2. 실행 순서 보장을 위해 진입 시점에 빈 Step을 먼저 리스트에 추가 (핵심 보완)
        Map<String, Object> step = new LinkedHashMap<>();
        if (scenarioContext.get() != null) {
            List<Map<String, Object>> steps = (List<Map<String, Object>>) scenarioContext.get().get("steps");
            steps.add(step);
        }
        // Kafka 발신 시 ID 주입 로직 유지
        if (className.contains("KafkaTemplate") && methodName.startsWith("send")) {
            injectIdToKafkaHeader(args);
        }
        String startTime = LocalDateTime.now(ZoneId.of("Asia/Seoul")).toString();
        Object result = null;
        Throwable exception = null;
        try {
            result = invocation.proceed(); // 실제 메서드 실행
            return result;
        } catch (Throwable t) {
            exception = t;
            throw t;
        } finally {
            if (scenarioContext.get() != null) {
                String endTime = LocalDateTime.now(ZoneId.of("Asia/Seoul")).toString();
                // 3. captureStep을 호출하여 미리 넣어둔 step 객체에 내용을 채움
                updateStepData(step, invocation, args, result, exception, isFirstEntry, startTime, endTime);
                if (isFirstEntry) {
                    Map<String, Object> ctx = scenarioContext.get();
                    List<Map<String, Object>> steps = (List<Map<String, Object>>) ctx.get("steps");
                    Map<String, Object> entryInfo = (Map<String, Object>) ctx.get("entryInfo");

                    // [수정] 추출된 entryInfo를 함께 전달
                    saveFullScenario((String) ctx.get("scenarioId"), entryInfo, steps);
                    scenarioContext.remove();
                }
            }
        }
    }

    private void updateStepData(Map<String, Object> step, MethodInvocation invocation, Object[] args,
                                Object result, Throwable exception, boolean isFirstEntry, String startTime, String endTime) {
        try {
            // [수정] 새로 생성(new LinkedHashMap)하지 않고 인자로 받은 step을 그대로 사용합니다.
            step.put("layer", deriveLayer(invocation));
            step.put("target", invocation.getMethod().getDeclaringClass().getName() + "." + invocation.getMethod().getName());
            step.put("startTime", startTime);
            step.put("endTime", endTime);

            if (args != null) {
                // 모든 인자를 순회하며 안전한 타입으로 변환
                List<Object> safeArgs = new ArrayList<>();
                for (Object arg : args) {
                    safeArgs.add(convertSafeValue(arg));
                    // Kafka 처리 로직 (기존 유지)
                    if (arg instanceof ConsumerRecord<?, ?> record) {
                        step.put("receivedFromTopic", record.topic());
                        Header srcHeader = record.headers().lastHeader("sourceService");
                        if (srcHeader != null) step.put("sourceService", new String(srcHeader.value()));
                    }
                }
                step.put("input", safeArgs);
            }
            if (exception != null) {
                step.put("status", "ERROR");
                step.put("message", exception.getMessage());
            } else {
                step.put("status", "SUCCESS");
                step.put("output", convertSafeValue(result)); // 결과값도 안전하게 변환
            }
        } catch (Exception e) {
            System.err.println("[Scenario Mining] Error: " + e.getMessage());
        }
    }

    private Object convertSafeValue(Object value) {
        if (value == null) return null;

        // [추가] Kafka ConsumerRecord 처리: 토픽 메시지 본문만 추출
        if (value instanceof org.apache.kafka.clients.consumer.ConsumerRecord) {
            org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record =
                    (org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>) value;
            return record.value(); // 실제 전송된 JSON 또는 문자열 본문 반환
        }
        // 기본 타입이나 단순 구조는 그대로 반환
        if (value instanceof String || value instanceof Number || value instanceof Boolean ||
                value instanceof Map || value instanceof Collection) {
            return value;
        }

        // 직렬화 문제가 발생하는 특정 라이브러리 객체들은 문자열로 치환
        String className = value.getClass().getName();
        if (className.startsWith("org.springframework.http") ||
                className.startsWith("org.apache.kafka") ||
                className.startsWith("javax.servlet") ||
                className.startsWith("jakarta.servlet")) {
            return value.toString();
        }

        return value.toString(); // 일반 DTO 등은 그대로 반환 (FAIL_ON_EMPTY_BEANS 설정이 처리해줌)
    }

    private String findTraceIdFromArgs(Object[] args) {
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof ConsumerRecord<?, ?> record) {
                    Header header = record.headers().lastHeader("scenarioId");
                    if (header != null) return new String(header.value());
                }
            }
        }
        try {
            ServletRequestAttributes attrs = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs != null) return attrs.getRequest().getHeader("X-Scenario-Id");
        } catch (Exception ignored) {}
        return null;
    }

    private void injectIdToKafkaHeader(Object[] args) {
        Map<String, Object> context = scenarioContext.get();
        if (context == null || args == null) return;
        String sid = (String) context.get("scenarioId");
        for (Object arg : args) {
            if (arg instanceof ProducerRecord<?, ?> record) {
                // scenarioId -> X-Scenario-Id 로 변경 추천
                record.headers().add("X-Scenario-Id", sid.getBytes());
                if (this.currentServiceName != null) {
                    // sourceService -> X-Source-Service 로 변경하여 API 헤더와 통일
                    record.headers().add("X-Source-Service", this.currentServiceName.getBytes());
                }
            }
        }
    }

    private boolean isBusinessEntryPoint(String className, String methodName) {
        if (methodName.contains("after") || methodName.startsWith("set") || className.contains("Config")) return false;
        return className.endsWith("Controller") || className.endsWith("Service") ||
                className.endsWith("Consumer") || className.endsWith("Listener");
    }

    private String deriveLayer(MethodInvocation invocation) {
        String name = invocation.getMethod().getDeclaringClass().getSimpleName();
        if (name.contains("Controller")) return "API_ENTRY";
        if (name.contains("Service")) return "BUSINESS_LOGIC";
        // KafkaTemplate만 메시지 큐로 지정
        if (name.contains("KafkaTemplate")) return "MESSAGE_QUEUE";
        // RestTemplate이나 외부 호출 관련은 EXTERNAL_API로 분리
        if (name.contains("RestTemplate") || name.contains("Client")) return "EXTERNAL_API";
        if (name.contains("Mapper") || name.contains("Repository")) return "DATABASE";
        return "INTERNAL_CALL";
    }

    private void saveFullScenario(String scenarioId, Map<String, Object> entryInfo, List<Map<String, Object>> steps) throws IOException {
        if (this.serviceLogPath == null) return;
        // 1. 인입 지점(Entry Point) 정보 추출 (첫 번째 스텝이 보통 인입점)
        // steps의 첫 번째 항목에서 실제 경로 추출
        Map<String, Object> entryStep = (Map<String, Object>) steps.get(0);
        String service = this.currentServiceName;
        String target = (String) entryStep.get("target");
        // 마지막 마침표 위치 찾기
        int lastDot = target.lastIndexOf(".");
        String methodName = target.substring(lastDot + 1);          // handleStockResult
        String className = target.substring(0, lastDot); // OrderService

        // 1. 날짜/시간 포맷 정의 (yyyymmddhh24miss 형식)
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        // 2. 파일명 구성 (가독성과 검색을 위해 언더바로 구분)
        // 형식: FullScenario_서비스_클래스_메소드_ID_시간.json
        String fileName = String.format("%s_%s_%s_%s_%s.json",
                timestamp,
                scenarioId,
                service,
                className,
                methodName
        );

        File dir = new File(this.serviceLogPath);
        if (!dir.exists()) dir.mkdirs();

        Map<String, Object> full = new LinkedHashMap<>();
        full.put("scenarioId", scenarioId);
        full.put("entryInfo", entryInfo);
        full.put("steps", steps);

        Files.write(Paths.get(this.serviceLogPath + fileName), objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(full).getBytes());
    }
}