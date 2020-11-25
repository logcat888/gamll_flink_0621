package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenhuiup
 * @create 2020-11-25 11:59
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    // 定义输出数据类型
    private Long itemId;
    private Long windowEnd;  //窗口结束时间
    private Long count;
}
