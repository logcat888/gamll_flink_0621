package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenhuiup
 * @create 2020-11-25 19:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlViewCount {
   private String Url;
   private Long windowEnd;
   private Long count;
}
