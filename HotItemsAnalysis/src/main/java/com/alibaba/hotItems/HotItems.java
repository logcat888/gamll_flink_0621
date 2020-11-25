package com.alibaba.hotItems;

import com.alibaba.bean.ItemCount;
import com.alibaba.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author chenhuiup
 * @create 2020-11-25 11:49
 */
/*
需求：统计最近一小时热门商品TopN，每5分钟输出一次
实现思路：
1.使用什么时间语义：使用事件时间语义
2.观察数据源思考生成watermark的方式：由于数据已经经过ETL，成为有序的了，不存在乱序数据，所以可以使用升序提取watermark的方式获取watermark
3.按照itemID分组，定义滑动窗口(keyedWindow)，计数，使用aggregate（预聚合函数，窗口函数获取聚合后的值以及分配时间戳）
    1）reduce，sum等函数具有预聚合功能，来一条数据计算一条，但是无法获取窗口信息
    2）全窗口函数，能够获取窗口信息（开始时间，结束时间），但会将一个窗口的数据全部都放到迭代器中，没有预聚合功能。
    3）aggregate可以传入两个参数（AggregateFunction，WindowFunction）,AggregateFunction每来一条数据计算一条，并将最后的结果
        传递到WindowFunction的迭代器中，这样迭代器就只有一条数据，还能获取窗口时间。
4.按照窗口时间分组，为了将所有数据排序可以将窗口内的所有数据使用状态保存，等到收集到所有数据后再统一进行排序，然后输出topN。
    由于使用定时器，当定时时间到后对数据进行排序。定时器是根据定时时间唯一确定，窗口内的所有数据的标记都是结束时间，所有注册多个定时器。
    其实是同一个定时器。
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境，指定时间语义
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 2.从文件中读取数据转换为JavaBean，并提取时间生成watermark
        URL resource = HotItems.class.getResource("/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> inputDS = env.readTextFile(resource.getPath()).map(line -> {
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

        // 3.按照itemID分组，定义滑动窗口(keyedWindow)，计数，使用aggregate（预聚合函数，窗口函数获取聚合后的值以及分配时间戳）
        SingleOutputStreamOperator<ItemCount> aggDS = inputDS.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAggFunc(), new ItemWindowFunc());


        // 4.按照窗口结束时间分组，使用processAPI中定时器，统一输出数据，实现TopN
        SingleOutputStreamOperator<String> result = aggDS.keyBy(ItemCount::getWindowEnd)
                .process(new ItemCountProcessFunc(5));

        // 5.打印
        result.print();

        // 6.执行任务
        env.execute();
    }

    //定义增量聚合函数
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+ b;
        }
    }

    //定义窗口函数，获取增量聚合函数的结果，以及窗口结束信息
    // WindowFunction<IN, OUT, KEY, W extends Window>  IN为AggregateFunction聚合的结果， W当前是时间窗口
   public static class ItemWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        // 第一个参数key；第二个参数是window信息；第三个参数是聚合后的结果；第四个参数是发射数据
        @Override
        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            // Iterable<Long> input中只有一个值，聚合的结果，
            Long count = input.iterator().next();
            out.collect(new ItemCount(key,window.getEnd(),count));
        }
    }

    //实现一个窗口内的数据的排序
    public static class ItemCountProcessFunc extends KeyedProcessFunction<Long,ItemCount,String>{

        // 定义TopN属性
        private int topSize;

        public ItemCountProcessFunc() {
        }

        public ItemCountProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存窗口内的所有数据，等待定时时间到进行排序
        ListState<ItemCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("listState", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据就加入到列表中
            listState.add(value);
            //注册定时器,等窗口内的所有数据到齐后，value.getWindowEnd() + 1L，触发排序
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取状态
            Iterator<ItemCount> iterator = listState.get().iterator();
            // 将迭代器转换为ArrayList
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);
            // 排序,按照从大到小排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    return - o1.getCount().compareTo(o2.getCount());
                }
            });

            StringBuilder builder = new StringBuilder();
            builder.append("==============================\n");
            builder.append("窗口结束时间：").append(timestamp - 1L).append("\n");

            // 取topSize的item进行输出
            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                //取出数据
                ItemCount itemCount = itemCounts.get(i);
                builder.append("TOP ").append(i + 1);
                builder.append(" ItemId=").append(itemCount.getItemId());
                builder.append(" 商品热度=").append(itemCount.getCount());
                builder.append("\n");
            }
            builder.append("==============================\n");

            // 输出数据
            out.collect(builder.toString());

            //清空状态
            listState.clear();
            TimeUnit.SECONDS.sleep(1); //避免计算太快
        }
    }
}
