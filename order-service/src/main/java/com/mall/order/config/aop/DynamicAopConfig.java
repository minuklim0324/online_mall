package com.mall.order.config.aop;

import org.springframework.aop.aspectj.AspectJExpressionPointcut;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableAspectJAutoProxy
public class DynamicAopConfig {

    private final String INCLUDE_PATH = "C:/app/pilot-p/scenario/config/order_methods.txt";
    private final String EXCLUDE_PATH = "C:/app/pilot-p/scenario/config/exclude_methods.txt";

    @Value("${spring.application.name:unknown_service}")
    private String serviceName;

    @Bean
    public DefaultPointcutAdvisor scenarioAdvisor(ScenarioMiningInterceptor interceptor) {
        interceptor.initLogPath(serviceName);
        AspectJExpressionPointcut pointcut = new AspectJExpressionPointcut();
        pointcut.setExpression(buildFullExpression());
        return new DefaultPointcutAdvisor(pointcut, interceptor);
    }

    private String buildFullExpression() {
        String includes = loadPatterns(INCLUDE_PATH, true);
        String excludesFromFile = loadPatterns(EXCLUDE_PATH, false);

        // 1. 포함 패턴: 파일 내용이 없으면 기본 패키지 설정
        String finalIncludes;
        if (includes == null || includes.trim().isEmpty()) {
            finalIncludes = "(execution(* com.mall..*.*(..)))";
        } else {
            // 이미 execution이 포함된 문자열이므로 전체만 괄호로 감싸서 논리 연산 보호
            finalIncludes = "(" + includes + ")";
        }

        StringBuilder sb = new StringBuilder();

        // 2. 고정 제외 패턴: 괄호를 사용하여 각각의 단위를 완벽히 격리 (파서 오류 방지 핵심)
        // 에러가 났던 ..*set*(..) 대신 표준적인 *..set*(..) 형식을 사용합니다.
        sb.append("(!execution(* com.mall..config..*(..)))");
        sb.append(" && (!execution(* com.mall..interceptor..*(..)))");
        sb.append(" && (!execution(* *..after*(..)))");
        sb.append(" && (!execution(* *..set*(..)))");

        // 3. 파일에서 읽어온 추가 제외 패턴이 있다면 결합
        if (excludesFromFile != null && !excludesFromFile.trim().isEmpty()) {
            sb.append(" && (!(").append(excludesFromFile).append("))");
        }

        // 4. 최종 포함 패턴과 결합
        sb.append(" && ").append(finalIncludes);

        // 5. 모든 공백을 표준 스페이스 한 칸으로 치환하여 파싱 안정성 확보
        return sb.toString().replaceAll("\\s+", " ").trim();
    }

    private String loadPatterns(String filePath, boolean isInclude) {
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) return "";
            List<String> lines = Files.readAllLines(path).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toList());

            if (lines.isEmpty()) return "";
            return lines.stream().map(line -> {
                if (line.startsWith("execution")) return line;
                String suffix = (isInclude && line.contains("org.springframework")) ? "+" : "";
                return String.format("execution(* %s%s.*(..))", line, suffix);
            }).collect(Collectors.joining(" || "));
        } catch (IOException e) { return ""; }
    }
}