package com.alibaba.app;

import com.alibaba.bean.OrderEvent;
import com.alibaba.bean.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author chenhuiup
 * @create 2020-11-28 21:21
 */
/*
背景：对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，
所以我们要同时读入两条流的数据来做合并处理。这里我们利用connect将两条流进行连接，然后用自定义的CoProcessFunction进行处理。
需求：使用flink进行双流连接，判断支付和到账能够连接上。
整体思路：使用connect的方式
    1.读取两条流，使用connect进行连接，connect可以连接类型不同的两条流；而union只能连接相同类型的流。
    2.实现coMap中的两个方法，保存相应的状态；
        1）对于订单事件（pay），判断账单是否存在状态中，如果存在状态中，则证明关联上进行输出，否则，注册定时器，比如5秒
        2）对于账单事件，判断订单是否存在状态中，如果存在状态中，则证明关联上进行输出，否则，注册定时器，比如3秒
        3）按理说pay需要等待账单的时间长一些。
    3.当定时器响时，判断状态，
        1）如果订单状态不为空，则说明账单没有到，输出侧输出流中，标记为有订单没账单
        2）如果订单状态为空，则说明账单不为空，输出到侧输出流中，标记为有账单没订单
整体思路：使用join的方式，存在的问题就是如果没有join上的数据会丢失，而connect可以输出到侧输出流中。
    1.读取两条流，使用join进行连接
    2.between默认是左闭右闭，可以使用参数进行闭开的调整。
    3.join的两条流必须是keyBy之后的流。
存在的问题：由于是从文本文件中读取数据，而watermark默认是生成周期是200ms生成一个，文本文件读取过快，会出现，本不应该join上的数据
    join上了。但是在实际生产中不会出现这种情况。而join的方式是严格按照时间进行join，不会出现乱join的问题，只不过缺陷就是没有join上的
    数据会发生丢失。
 */
public class OrderReceiptAppWithConnect {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，指定事件时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从文本中读取数据，转换为JavaBean对象，提取时间戳和指定生成watermark
        URL resource = OrderTimeoutAppWithCep.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile(resource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
        }).filter(data -> !"".equals(data.getEventType())) //过滤出支付订单
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });
        URL receiptLog = OrderTimeoutAppWithCep.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile(receiptLog.getPath()).map(line -> {
            String[] split = line.split(",");
            return new ReceiptEvent(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 3.按照流水账单号分组，连接订单流和账单流
        KeyedStream<OrderEvent, String> orderEventKeyedStream = orderEventDS.keyBy(OrderEvent::getTxId);
        KeyedStream<ReceiptEvent, String> receiptEventKeyedStream = receiptEventDS.keyBy(ReceiptEvent::getTxId);
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultDS =
                orderEventKeyedStream.connect(receiptEventKeyedStream).process(new CoProcessFunc());

        // 4.打印数据
        resultDS.print("有订单，有账单");
        resultDS.getSideOutput(new OutputTag<String>("PayButNoReceipt") {
        }).print("账单丢了");
        resultDS.getSideOutput(new OutputTag<String>("ReceiptButNoPay") {
        }).print("订单丢了");

        // 5.执行任务
        env.execute();
    }

    public static class CoProcessFunc extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        //定义状态保存
        ValueState<OrderEvent> orderState;
        ValueState<ReceiptEvent> receiptState;

        //定时器
        ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("OrderState", OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("ReceiptState", ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //判断账单状态是否为空，如果不为空，说明能够关联上，输出到主流
            ReceiptEvent receiptEvent = receiptState.value();
            if (receiptEvent != null) {
                out.collect(new Tuple2<>(value, receiptEvent));
                //删除定时器,清空状态
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                orderState.clear();
                receiptState.clear();
                tsState.clear();
            } else {
                //如果为空，说明账单还没有到，需要等到5秒，并把订单存入状态
                orderState.update(value);
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }
        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //判断订单状态是否为空，如果不为空，说明能够关联上，输出到主流
            OrderEvent orderEvent = orderState.value();
            if (orderEvent != null) {
                out.collect(new Tuple2<>(orderEvent, value));
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                orderState.clear();
                receiptState.clear();
                tsState.clear();
            } else {
                //如果为空，说明订单还没有到，需要等到5秒，并把账单存入状态
                receiptState.update(value);
                long ts = (value.getTimestamp() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            OrderEvent orderEvent = orderState.value();
            if (orderEvent != null) {
                //如果订单不为空，说明账单丢了，订单输出到侧输出流
                ctx.output(new OutputTag<String>("PayButNoReceipt") {
                }, orderEvent.getTxId() + "账单丢了");
            } else {
                //订单丢了
                if (receiptState.value() != null) {
                    String txId = receiptState.value().getTxId();
                    ctx.output(new OutputTag<String>("ReceiptButNoPay") {
                    }, txId + "订单丢了");
                }
            }
            //清空状态
            orderState.clear();
            receiptState.clear();
        }



    }
}
