package com.alibaba.app;

import com.alibaba.bean.OrderEvent;
import com.alibaba.bean.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-11-28 21:20
 */
/*
需求：订单超时统计，15分钟算作超时，使用CEP筛选出超时的订单
为什么使用Flink做订单超时的检测？主要目的就是为了减轻业务系统的压力，对于订单超时的检测不是业务系统的核心任务，业务系统需要不停的
    轮循判断订单是否超时，消耗性能，因此，可以将这部分业务抽离出来，交给大数据实时处理，如果订单超时或完成订单，大数据可以直接修改
    数据库中的订单状态。业务系统直接获取数据即可。
整体思路：使用CEP
    1.数据来源：文本文件，没有乱序数据
    2.使用CEP定义模式序列，应用到流上（经过keyBy，按照orderId），使用select拣选超时事件与匹配事件，分别进行处理，超时事件输出到侧输出流中。
    3.使用宽松近邻定义模式序列，因为订单的创建和支付之间可能会穿插其他事件。
    4.java的泛型方法是写在方法名前
    5.CEP可以作用的流可以是普通流，也可以是keyBy之后的流。具体要根据逻辑选择，这里只有同一个订单的事件匹配才有意义。
整体思路：使用ProcessAPI
    1.读取数据，提取事件时间戳后，按照OrderId进行分组，自定义Process API
    2.主要分为3种情况
         1）定义值状态，保存create，并注册15分钟定时器。
       2）如果15分钟内pay过来，判断是否有create
            2.1）如果有，输出数据，并删除定时器，清空状态。
            2.2）如果没有，输出到侧输出流，正常逻辑不应该出现有pay而没有create的情况。由于是自己造的数据，所以可能会出现这种情况
总结：Process API能够实现的逻辑会比CEP更加全面，可以将有问题的数据输出到侧输出流，而CEP就不能实现，但是CEP能够自己管理状态，代码逻辑简单。

 */
public class OrderTimeoutAppWithCep {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，指定事件时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2.从文本文件中读取数据，转换为JavaBean对象，提取时间戳和指定生成watermark
        URL resource = OrderTimeoutAppWithCep.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile(resource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(Long.parseLong(fields[0]), fields[1], fields[2], Long.parseLong(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000L;
            }
        });

        // 3.定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 4.将模式序列应用到流上,注意这里的逻辑流是keyBy之后的流
        PatternStream<OrderEvent> patternDS = CEP.pattern(orderEventDS.keyBy(data -> data.getOrderId()), pattern);

        // 5.使用select拣选超时事件和成功支付的事件，超时事件会输出到侧输出流中。
        SingleOutputStreamOperator<OrderResult> selectDS = patternDS.select(new OutputTag<OrderResult>("timeout") {
        }, new OrderTimeOutFunc(), new OrderSelectFunc());

        // 6.打印
        selectDS.print("成功支付");
        selectDS.getSideOutput(new OutputTag<OrderResult>("timeout") {}).print("超时订单");

        // 7.执行
        env.execute();
    }

    //自定义超时事件处理逻辑
    public static class OrderTimeOutFunc implements PatternTimeoutFunction<OrderEvent,OrderResult>{

        // 1.map：由于是超时事件，肯定只有create，而没有pay,且list中只有一个元素
        // 2.timeout：是超时时间结束的时间点，这里我们设置的是15分钟
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timeout) throws Exception {
            // 获取匹配到的事件
            List<OrderEvent> create = map.get("create");
            Long orderId = create.get(0).getOrderId();
            return new OrderResult(orderId,"timeout");
        }
    }

    //自定义正常事件处理逻辑
    public static class OrderSelectFunc implements PatternSelectFunction<OrderEvent,OrderResult>{

        // 1.map中可以获取create和pay的信息
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            // 获取匹配到的事件
            List<OrderEvent> create = map.get("create");
            Long orderId = create.get(0).getOrderId();
            return new OrderResult(orderId,"pay");
        }
    }

}
