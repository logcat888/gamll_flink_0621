package com.alibaba.app;

import com.alibaba.bean.PvCount;
import com.alibaba.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author chenhuiup
 * @create 2020-11-25 20:36
 */
/*
需求：统计每小时内的网站PV，pv是用户每点击一次页面都算一次点击量
主要思路：
1.数据来源：日志服务器产生的用户行为数据，已经做了ETL，时间有序，过滤出pv数据；
2.过滤出pv,设置dummy key ,设置窗口大小为1小时，使用sum聚合（或reduce聚合，可以加上窗口信息），输出
存在的问题：由于设置了同一个key，所以会发生数据倾斜，所有数据都发往同一个slot上。
解决思路：
    1）根据并行度，随机产生key，比如（0，最大并行度），添加上窗口信息，聚合；
    2）按照窗口结束时间分组，最次聚合，输出窗口内的数据。

 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，指定时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2.从文件中读取数据转换为JavaBean，并提取时间生成watermark
//        URL resource = PageViewApp.class.getResource("/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv");
        String path = "D:\\workspace_idea\\gamll_flink_0621\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv";
        SingleOutputStreamOperator<UserBehavior> inputDS = env.readTextFile(path).map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
            // 由于数据已经经过ETL，成为有序的了，不存在乱序数据，所以可以使用升序提取watermark的方式获取watermark
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 3.过滤出pv,设置dummy key ,设置窗口大小为1小时，使用sum聚合，输出
        SingleOutputStreamOperator<PvCount> pvDS = inputDS.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<String, Long>("pv", 1L);
                    }
                })
                .keyBy(data -> data.f0) //存在的问题，并行度丢失
                .timeWindow(Time.hours(1))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }, new WindowFunction<Tuple2<String, Long>, PvCount, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<PvCount> out) throws Exception {
                        // 获取count
                        Tuple2<String, Long> data = input.iterator().next();
                        out.collect(new PvCount(window.getEnd(), data.f1));
                    }
                });

        // 4.打印
        pvDS.print();

        // 5.执行
        env.execute();
    }
}