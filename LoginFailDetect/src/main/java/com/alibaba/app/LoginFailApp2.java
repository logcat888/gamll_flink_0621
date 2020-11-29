package com.alibaba.app;

import com.alibaba.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;

/**
 * @author chenhuiup
 * @create 2020-11-27 20:09
 */
/*
需求：恶意登录检测，如果用户连续两秒内登录失败超过2次，就认为恶意登录
错误1：
整体思路：按照用户分组，实现process API
    1.定义状态保存用户登录失败，定义状态保存定时器注册时间
    2.如果有登录成功的消息，就清空状态和删除定时器；
    3.对于第一条失败的登录，注册定时器，并保留状态
    4.等到定时器触发时，判断保存的状态是否大于2，如果大于，输出恶意登录警告
存在的问题：如果黑客在1秒内连续失败1万次，终于在1.99s时登录成功，这时就删除定时器和清空状态，不会输出报警信息。
进阶2：不定义定时器，当收到第二条失败信息时判断两条失败的信息的时间差是否小于2秒，如果小于就输出报警信息。当有登录成功的信息就清空状态。
存在的问题：如果需求更改为两秒内失败登录超过5次，实现的逻辑就非常复杂，当到来了失败消息，需要与第一条失败消息判断是否小于2秒钟，如果不小于
    就删除第一条失败消息，且依次判断第二条，第三条。实现的逻辑就非常复杂。
进阶3：使用CEP匹配复杂事件
    1）CEP会根据定义的模式序列，实现状态的管理，CEP底层的实现就是非限制性有限状态机（NFA）实现对状态的管理。
    2）CEP也会根据watermark，处理延迟数据/乱序数据，只有watermark推进时才会将watermark到达的时间点的数据匹配到流中。
    3)java中泛型方法的泛型写在方法名的前面
 */
public class LoginFailApp2 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        // 2.从流中读取数据，提取时间戳，并生成watermark，指定乱序程度为3秒
        URL resource = LoginFailApp.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile(resource.getPath()).map(line -> {
            String[] split = line.split(",");
            return new LoginEvent(Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 3.按照用户分组，实现process API，将报警信息输出到主流
        SingleOutputStreamOperator<String> result = loginEventDS.keyBy(LoginEvent::getUserId)
                .process(new LoginFailProcessFunc());

        // 4.打印
        result.print();

        // 5.执行任务
        env.execute();
    }

    public static class LoginFailProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义状态保存用户失败信息
        ListState<LoginEvent> failState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failState", LoginEvent.class));
        }


        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            // 获取状态
            Iterable<LoginEvent> loginEvents = failState.get();
            if ("fail".equals(value.getEventType())){
                // 如果是失败消息,判断是否是第一条，如果是则加入状态
                if (!loginEvents.iterator().hasNext()){
                    failState.add(value);
                }else {
                    // 如果不是第一条判断是否时间小于2秒，如果小于则输出报警信息，否则清空状态，加入新来的失败消息
                    LoginEvent first = loginEvents.iterator().next();
                    if (first.getTimestamp() + 2 > value.getTimestamp()){

                        out.collect(ctx.getCurrentKey() +
                                "在" + first.getTimestamp() +
                                "到" + value.getTimestamp() +
                                "之间登录失败2次！");
                    }
                        failState.clear();
                        failState.add(value);
                }
            }else {
                //成功登陆,清空状态
                failState.clear();
            }
        }

    }
}

