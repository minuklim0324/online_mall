package com.mall.payment.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface PaymentMapper {
    // XML의 id와 메소드명을 일치시킵니다.
    void insertPayment(@Param("orderId") Long orderId, 
                       @Param("amount") double amount, 
                       @Param("status") String status);

    void updateStatus(@Param("orderId") Long orderId, 
                      @Param("status") String status);

    String findStatusByOrderId(@Param("orderId") Long orderId);
}