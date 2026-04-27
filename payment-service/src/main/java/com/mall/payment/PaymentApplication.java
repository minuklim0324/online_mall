package com.mall.payment;

import jakarta.annotation.PostConstruct; // 추가
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone; // 추가

@SpringBootApplication
public class PaymentApplication {

    @PostConstruct
    public void started() {
        // 애플리케이션의 기본 타임존을 한국 시간(KST)으로 설정
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
        System.out.println("Current TimeZone: " + TimeZone.getDefault().getID());
    }

    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}