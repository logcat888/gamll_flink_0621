package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenhuiup
 * @create 2020-11-25 18:52
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
