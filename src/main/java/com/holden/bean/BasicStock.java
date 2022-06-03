package com.holden.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName stock_flink-BasicStock
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月03日18:30 - 周五
 * @Describe
 */

@Data
public class BasicStock {
    Long number;
    Integer type;
    Integer market;
    String code;
    String name;//订单中商品总金额
    BigDecimal dk_total;
    BigDecimal turnover_rate;
    BigDecimal highest;
    BigDecimal lowest;
    BigDecimal opening_price;
    BigDecimal closing_price;
    BigDecimal deal_amount;
    String year;//yyyy
    String month_day; //MM-dd
}
