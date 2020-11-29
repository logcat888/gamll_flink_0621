package com.alibaba.app;

import com.alibaba.bean.AdClickEvent;
import com.alibaba.bean.AdCountByProvince;
import com.alibaba.bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

/**
 * @author chenhuiup
 * @create 2020-11-27 18:47
 */
/*
需求：统计最近一小时，各省份页面广告点击量统计，每5秒钟统计一次，对重复点击广告超过100次的用户拉入黑名单
整体思路：
    1.观察数据源：发现数据的时间戳是升序的，都指定生成watermark为升序，不设置最大乱序程度
    2.自定义process过滤，黑名单判断：按照用户和广告分组，计算用户是否点击同一条广告超过100次，如果超过拉入黑名单即输出到侧输出流
    3.按照省份分组，开滑动窗口，统计广告点击次数，补充窗口信息
注意：侧输出流必须加{}，new OutputTag<BlackListWarning>("outPut"){}
 */
public class AdClickByProvinceApp {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，设置时间语义为事件时间
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从文件中读取数据，提取时间戳，并生成watermark
        URL resource = AdClickByProvinceApp.class.getResource("/AdClickLog.csv");
        SingleOutputStreamOperator<AdClickEvent> inputDS = env.readTextFile(resource.getPath()).map(data -> {
            String[] fields = data.split(",");
            return new AdClickEvent(Long.parseLong(fields[0]),
                    Long.parseLong(fields[1]),
                    fields[2],
                    fields[3],
                    Long.parseLong(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 3.自定义process过滤，黑名单判断
        SingleOutputStreamOperator<AdClickEvent> filterDS = inputDS.keyBy("userId", "adId")
                .process(new AdClickFilter(100));

        // 4.按照省份分组
        SingleOutputStreamOperator<AdCountByProvince> resultDS = filterDS.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdCliAgg(), new AdClickWindowFunc());

        // 5.打印
        resultDS.print("result");
        // 打印侧输出流
        filterDS.getSideOutput(new OutputTag<BlackListWarning>("outPut"){}).print("outPut");

        // 6.执行
        env.execute();


    }


    // 自定义过滤，每天监测用户是否需要加入黑名单，每天的0点清空状态
    public static class AdClickFilter extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {

        //定义状态，保存用户对同一条广告的点击次数
        ValueState<Long> countState;
        //定义状态，判断用户是否加入黑名单
        ValueState<Boolean> isBlackState;
        // 限制次数
        private int maxSize;

        public AdClickFilter() {
        }

        public AdClickFilter(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-State", Long.class));
            isBlackState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isBlackState", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {

            // 获取状态
            Boolean isBlack = isBlackState.value();
            Long count = countState.value();
            if (isBlack == null) {
                //如果是第一条数据,初始化黑名单和更新状态，注册定时器
                isBlackState.update(false);
                countState.update(1L);
                long nextDay = (value.getTimestamp() / (60 * 60 * 24) + 1) * (60 * 60 * 24 * 1000) - (60 * 60 * 8 * 1000);
                ctx.timerService().registerEventTimeTimer(nextDay);
                out.collect(value);
            } else if (!isBlack) {
                //如果不在黑名单中,再判断count是否大于100,如果大于则加入黑名单，并输出到侧输出流
                if (count != null && count >= maxSize) {
                    isBlackState.update(true);
                    ctx.output(new OutputTag<BlackListWarning>("outPut"){}, new BlackListWarning(value.getUserId(), value.getAdId(), "warning"));
                    return;
                }
                if (count != null){
                    countState.update(count + 1);
                }
                out.collect(value);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isBlackState.clear();
        }
    }

    public static class AdCliAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
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

    public static class AdClickWindowFunc implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountByProvince(province,windowEnd,count));
        }
    }

}
