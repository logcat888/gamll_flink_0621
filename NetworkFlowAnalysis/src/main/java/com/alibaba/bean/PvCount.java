package com.alibaba.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author chenhuiup
 * @create 2020-11-25 20:56
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PvCount {
    private Long windowEnd;
    private Long count;

    @Override
    public String toString(){
        Timestamp timestamp = new Timestamp(windowEnd);
        return timestamp + "," + count;
    }
}
