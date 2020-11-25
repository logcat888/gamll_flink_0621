package com.alibaba.app;

import com.alibaba.bean.ApacheLog;
import com.alibaba.bean.UrlViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author chenhuiup
 * @create 2020-11-25 18:41
 */
/*
需求：统计最近10分钟内热门页面topN，每5秒钟统计一次
数据来源：来自于Apache日志服务器。由于前端没有进行页面埋点，但是Apache服务器发送请求时有加载网络时发送的请求（加载图片，视频等），
        但是这些并不是用户点击产生的日志，可以使用正则表达式进行过滤（.png...等等）。如果有埋点日志肯定不使用Apache服务器产生的
        日志，请求主要是（GET/POST/PUT/DELETE），再这里简单的过滤GET请求的数据。
1.下面这种方式的问题：
由于数据观察到有1分钟左右的延迟，设置watermark延迟为1分钟，这非常不恰当。因为滑动步长为5秒，说明对实时性要求非常高，因此
watermark的延迟可以设置为一个可以包含大部分数据延迟的情况，比如1秒。这样数据的准确性就会下降，但是没有办法，为了追求实时性
准确性肯定会下降。当然也会有一部分数据丢失，为了保证数据不丢失可以使用允许迟到数据和侧输出流的方式保证结果的正确性。设置迟到
数据1分钟。设置了1分钟的迟到数据，当watermark到达窗口时间就会触发计算，之后进行排序，然后清空状态。如果有迟到的数据，
在迟到允许范围内就会再次注册定时器，添加到list状态中，这样数据就有问题，出现之前窗口的数据输出一次就丢失。
2.改进的思路是：
设置watermark最大乱序程度为1s，1分钟的允许迟到数据，清空状态的时间为在允许迟到的时间到时（比如1分钟）再清空状态。
但是如果继续使用list保存状态就会出现问题，没有时间的数据会统一保存一次状态，而迟到的数据每来一次保存一次，
虽然之前的聚合状态还是正确的，但是list中存的数据就可能出现同一个url，保存多个count。需要使用map状态保存url和count值。
3. 在事件时间语义下，什么数据进入侧输出流？什么情况下会丢失数据？
   网络延迟会导致数据在传输过程中发生乱序，因此使用事件时间语义真实反应数据产生时间。flink是一个流式处理框架，为了追求实时性，
   结果的正确性就会降低可以通过设置watermark最大乱序程度、允许迟到数据、以及输出到侧输出流的方式保证数据不丢失。
   1）watermark最大乱序程度的设置应该能够包含大部分迟到数据，设置的过大实时性降低，设置的过小正确性会降低。watermark到了窗口时间
   就会触发窗口计算。
   2）允许迟到数据，在迟到允许时间内到达的迟到数据还会和之前的窗口数据一起计算，只是每来一次就会计算一次，但是必须有watermark推进时
   才会触发计算；
   3）侧输出流：当迟到数据属于的所有时间窗口都关闭时就会进入侧输出流，只要有一个窗口没有关闭，就不会进入侧输出流。
   4）丢失数据的情况：
     4.1）滚动窗口：设置了侧输出流，对于输出到侧输出流迟到的数据直接分配到对应的窗口就行，不存在丢失数据的情况。
     4.2）滑动窗口：
        4.2.1）没有设置允许迟到数据，设置了侧输出流：不会丢失数据，对于输出到侧输出流迟到的数据直接分配到对应的窗口就行。
        4.2.2）设置了允许迟到数据，设置了侧输出流：会丢失数据，当迟到数据到来的时间大于A窗口允许迟到的时间，但是能够进入B窗口，
            那么数据就不会进入侧输出流，对于A窗口来说数据就丢失了，最终结果的正确性就会下降。

 */
public class HotUrlApp2 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，指定时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2.从文件中读取数据转换为JavaBean，并提取时间生成watermark
//        URL resource = HotUrlApp.class.getResource("NetworkFlowAnalysis/src/main/resources/apache.log");
        SingleOutputStreamOperator<ApacheLog> inputDS = env.readTextFile("NetworkFlowAnalysis/src/main/resources/apache.log").map(line -> {
            String[] split = line.split(" ");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long time = sdf.parse(split[3]).getTime();
            return new ApacheLog(split[0], split[1], time, split[5], split[6]);
            // 由于数据观察到有1分钟左右的延迟，设置watermark延迟为1分钟，这非常不恰当
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLog element) {
                return element.getEventTime();
            }
        });

        OutputTag<ApacheLog> outputTag = new OutputTag<ApacheLog>("late") {};

        // 3.过滤，按照url分组，使用滑动窗口，使用aggregate聚合，补充窗口结束时间
        SingleOutputStreamOperator<UrlViewCount> aggDS = inputDS.filter(data -> "GET".equals(data.getMethod()))
                .keyBy(ApacheLog::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(outputTag)
                .aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());


        // 4.按照窗口结束时间分组，使用process API收集窗口内的所有数据，然后进行排序topN
        SingleOutputStreamOperator<String> result = aggDS.keyBy(UrlViewCount::getWindowEnd)
                .process(new UrlCountProcessFunc(5));

        // 5.打印
        result.print();

        // 获取侧输出流数据
        aggDS.getSideOutput(outputTag).print("side");

        // 6.执行
        env.execute();
    }


    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator + 1L;
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

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    public static class UrlCountProcessFunc extends KeyedProcessFunction<Long, UrlViewCount, String> {

        // 定义TopN属性
        private int topSize;

        public UrlCountProcessFunc() {
        }

        UrlCountProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        // 定义map状态，保存窗口内的所有数据，等待定时时间到进行排序
        MapState<String, UrlViewCount> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
         mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlViewCount>("mapState", String.class, UrlViewCount.class));
        }


        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {


            //每来一条数据就加入到map状态中
            mapState.put(value.getUrl(),value);
            //注册定时器,等窗口内的所有数据到齐后，value.getWindowEnd() + 1L，触发排序
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
            //注册定时器，清空状态
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp == ctx.getCurrentKey() + 60000L){
                //清空状态
                mapState.clear();
                //结束方法
                return;
            }

            // 获取状态
            Iterator<Map.Entry<String, UrlViewCount>> iterator = mapState.entries().iterator();

            // 将迭代器转换为ArrayList
            ArrayList<Map.Entry<String, UrlViewCount>> entries = Lists.newArrayList(iterator);
            // 排序,按照从大到小排序
            entries.sort(new Comparator<Map.Entry<String, UrlViewCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlViewCount> o1, Map.Entry<String, UrlViewCount> o2) {
                    return -o1.getValue().getCount().compareTo(o2.getValue().getCount());
                }
            });

            StringBuilder builder = new StringBuilder();
            builder.append("==============================\n");
            builder.append("窗口结束时间：").append(new Timestamp(timestamp - 1L)).append("\n");

            // 取topSize的item进行输出
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                //取出数据
                UrlViewCount itemCount = entries.get(i).getValue();
                builder.append("TOP ").append(i + 1);
                builder.append(" Url ").append(itemCount.getUrl());
                builder.append(" Url热度=").append(itemCount.getCount());
                builder.append("\n");
            }
            builder.append("==============================\n");

            // 输出数据
            out.collect(builder.toString());


            TimeUnit.SECONDS.sleep(1); //避免计算太快
        }
    }
}
