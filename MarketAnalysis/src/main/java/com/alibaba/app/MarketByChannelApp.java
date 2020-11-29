package com.alibaba.app;

import com.alibaba.bean.ChannelBehaviorCount;
import com.alibaba.bean.MarketUserBehavior;
import com.alibaba.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author chenhuiup
 * @create 2020-11-27 18:16
 */
/*
需求：市场营销需求：统计最近一小时各渠道用户行为次数，每隔5秒展示一次
数据来源：自定义source，产生数据
整体思路：按照渠道和行为进行分组，滑动窗口，使用aggregate，附带窗口信息
 */
public class MarketByChannelApp {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.获取数据
        SingleOutputStreamOperator<MarketUserBehavior> inputDS = env.addSource(new MarketBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        // 3.分组开窗
        SingleOutputStreamOperator<ChannelBehaviorCount> aggregateDS = inputDS
                .keyBy(new KeySelector<MarketUserBehavior, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketAgg(), new MarketWindowFunc());

        // 4.打印
        aggregateDS.print();

        // 5.执行任务
        env.execute();
    }

    public static class MarketAgg implements AggregateFunction<MarketUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class MarketWindowFunc implements WindowFunction<Long, ChannelBehaviorCount, Tuple2<String,String>, TimeWindow> {

        @Override
        public void apply(Tuple2<String,String> tuple2, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {
            Long count = input.iterator().next();
            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new ChannelBehaviorCount(tuple2.f0,tuple2.f1,windowEnd,count));
        }
    }
}
