package com.mall.order.repository;

import com.mall.order.dto.OrderRequest;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface OrderMapper {
    int insertOrder(OrderRequest req);
    
    void updateStatus(@Param("orderId") Long orderId, @Param("status") String status);

    // 멱등성 체크를 위한 상태 조회 로직
    @Select("SELECT status FROM orders WHERE order_id = #{orderId}")
    String findStatusById(Long orderId);
}