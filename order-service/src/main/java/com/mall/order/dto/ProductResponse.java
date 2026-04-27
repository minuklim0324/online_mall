package com.mall.order.dto;
import lombok.Data;

@Data
public class ProductResponse {
    private Long id;
    private String name;
    private Long price;
    private Integer stockCnt;
}