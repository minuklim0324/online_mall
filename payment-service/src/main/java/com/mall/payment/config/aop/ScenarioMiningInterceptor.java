package com.mall.payment.config.aop;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.*;

@Component
public class ScenarioMiningInterceptor implements MethodInterceptor {

    private final ObjectMapper objectMapper = new ObjectMapper();
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

    @Override
    public Object invoke(@NonNull MethodInvocation invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        String className = (invocation.getThis() != null) ? invocation.getThis().getClass().getSimpleName() : "Unknown";
        String methodName = invocation.getMethod().getName();

        boolean isFirstEntry = false;
        // 비즈니스 진입점 판단 (초기화 메서드 제외)
        if (scenarioContext.get() == null && isBusinessEntryPoint(className, methodName)) {
            String extractedId = findTraceIdFromArgs(args);
            Map<String, Object> context = new HashMap<>();
            context.put("scenarioId", (extractedId != null) ? extractedId : "TRX-" + System.currentTimeMillis());
            context.put("steps", new ArrayList<Map<String, Object>>());
            scenarioContext.set(context);
            isFirstEntry = true;
        }

        // Kafka 발신 시 ID 주입
        if (className.contains("KafkaTemplate") && methodName.startsWith("send")) {
            injectIdToKafkaHeader(args);
        }

        String startTime = LocalDateTime.now(ZoneId.of("Asia/Seoul")).toString();
        Object result = null;
        Throwable exception = null;

        try {
            result = invocation.proceed();
            return result;
        } catch (Throwable t) {
            exception = t;
            throw t;
        } finally {
            if (scenarioContext.get() != null) {
                String endTime = LocalDateTime.now(ZoneId.of("Asia/Seoul")).toString();
                captureStep(invocation, args, result, exception, className, methodName, isFirstEntry, startTime, endTime);
            }
        }
    }

    private void captureStep(MethodInvocation invocation, Object[] args, Object result, Throwable exception,
                             String className, String methodName, boolean isFirstEntry, String startTime, String endTime) {
        try {
            Map<String, Object> currentContext = scenarioContext.get();
            if (currentContext == null) return;

            List<Map<String, Object>> steps = (List<Map<String, Object>>) currentContext.get("steps");
            Map<String, Object> step = new LinkedHashMap<>();
            step.put("layer", deriveLayer(invocation));
            step.put("target", invocation.getMethod().getDeclaringClass().getName() + "." + methodName);
            step.put("startTime", startTime);
            step.put("endTime", endTime);

            if (args != null) {
                step.put("input", args);
                for (Object arg : args) {
                    if (arg instanceof ConsumerRecord<?, ?> record) {
                        step.put("receivedFromTopic", record.topic());
                        Header srcHeader = record.headers().lastHeader("sourceService");
                        if (srcHeader != null) step.put("sourceService", new String(srcHeader.value()));
                    }
                }
            }

            if (exception != null) {
                step.put("status", "ERROR");
                step.put("message", exception.getMessage());
            } else {
                step.put("status", "SUCCESS");
                step.put("output", result);
            }

            steps.add(step);

            if (isFirstEntry) {
                saveFullScenario((String) currentContext.get("scenarioId"), steps);
                scenarioContext.remove();
            }
        } catch (Exception e) {
            System.err.println("[Scenario Mining] Error: " + e.getMessage());
        }
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
                record.headers().add("scenarioId", sid.getBytes());
                if (this.currentServiceName != null) {
                    record.headers().add("sourceService", this.currentServiceName.getBytes());
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
        if (name.contains("Kafka") || name.contains("Template")) return "MESSAGE_QUEUE";
        if (name.contains("Mapper") || name.contains("Repository")) return "DATABASE";
        return "INTERNAL_CALL";
    }

    private void saveFullScenario(String scenarioId, List<Map<String, Object>> steps) throws IOException {
        if (this.serviceLogPath == null) return;
        File dir = new File(this.serviceLogPath);
        if (!dir.exists()) dir.mkdirs();
        Map<String, Object> full = new LinkedHashMap<>();
        full.put("scenarioId", scenarioId);
        full.put("steps", steps);
        String fileName = "FullScenario_" + scenarioId + "_" + System.currentTimeMillis() + ".json";
        Files.write(Paths.get(this.serviceLogPath + fileName), objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(full).getBytes());
    }
}