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
public class LoginFailApp {
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
        //定义状态保存定时器时间
        ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failState", LoginEvent.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            Iterable<LoginEvent> loginEvents = failState.get();

            if ("fail".equals(value.getEventType())) {
                if (!loginEvents.iterator().hasNext()){
//                if (loginEvents == null){  // 迭代器肯定不为空，只能通过上面的方法判断第一个元素有没有
                    //如果第一条失败信息，注册定时器，添加到状态中
                    long ts = value.getTimestamp() * 1000L + 2000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerState.update(ts);
                }
                failState.add(value);
            }else {
                Long timer = timerState.value();
                if (timer != null){
                    //清空状态，删除定时器
                    ctx.timerService().deleteEventTimeTimer(timer);
                }
                failState.clear();
                timerState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterable<LoginEvent> loginEvents = failState.get();
            ArrayList<LoginEvent> loginEventList = Lists.newArrayList(loginEvents.iterator());
            int size = loginEventList.size();
            if (size >= 2){
                LoginEvent firstFail = loginEventList.get(0);
                LoginEvent lastFail = loginEventList.get(size - 1);
                out.collect(ctx.getCurrentKey() +
                        "在" + firstFail.getTimestamp() +
                        "到" + lastFail.getTimestamp() +
                        "之间登录失败" + size + "次！");
            }
            //清空状态
            failState.clear();
            timerState.clear();
        }


        /*
        //定义状态数据
        private ListState<LoginEvent> listState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-state", LoginEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<>("ts-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            //取出状态数据
            Iterable<LoginEvent> loginEvents = listState.get();

            //判断是否为空,确定是否为第一条失败数据
            if (!loginEvents.iterator().hasNext()) {

                //第一条数据,则判断当前数据是否为登录失败
                if ("fail".equals(value.getEventType())) {

                    //将当前数据放置状态中,注册2s后的定时器
                    listState.add(value);

                    long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);

                    tsState.update(ts);
                }
            } else {

                //不是第一条数据,并且是失败数据
                if ("fail".equals(value.getEventType())) {
                    listState.add(value);
                } else {
                    //不是第一条数据,并且是成功数据,删除定时器
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //清空状态
                    listState.clear();
                    tsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的数据
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(listState.get().iterator());
            int size = loginEvents.size();

            //如果集合中数据大于等于2,则输出报警信息
            if (size >= 2) {
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(size - 1);
                out.collect(ctx.getCurrentKey() +
                        "在" + firstFail.getTimestamp() +
                        "到" + lastFail.getTimestamp() +
                        "之间登录失败" + size + "次！");
            }

            //清空状态
            listState.clear();
            tsState.clear();

        }

         */
    }
}
