package com.alibaba.hotItems;

import com.alibaba.bean.UserBehavior;
import com.alibaba.bean.UvCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author chenhuiup
 * @create 2020-11-27 13:28
 */
/*
UV指的是一段时间（比如一小时）内访问网站的总人数,去重
整体思路：
1.数据来源：经过ETL清洗，有序的数据
2.一小时滚动窗口，应用全窗口函数，收集到所有用户，使用HashSet对用户去重，统计用户的个数
存在的问题：
    1）全窗口函数会将一个窗口的所有数据收集到迭代器中，等到窗口关闭时才会触发计算，计算效率低
    2）使用HashSet对用户去重，需要将迭代器中的数据加载到内存，占用内存空间，当用户量大时容易OOM。比如1亿用户，每个用户信息20字节，
        需要的内存空间是 10^8 * 20 B = 10^6 * 2kB = 10^3 * 2MB = 2G
    3) 即便是将用户存在redis中也是对内存消耗非常大。
解决思路：
    1）使用布隆过滤器实现用户去重，在redis中存入String类型，key为窗口日期，value进行BitMap操作，由于redis是动态扩展存储空间，因此
      节省空间；
    2）自定义Bloom过滤器，定义Bloom过滤器的长度，实现一个hash算法计算用户在数组中的offset，之后用offset在redis进行BitMap操作
    3）使用process API进行全窗口的操作，但是自定义触发器，每来一条数据就计算一次，并发送数据；
    4）自定义全窗口processFunction，实现将用户写入到redis，并将UVcount保存在redis中。UvCount的保存按理说应该使用String类型，
        但是这里使用hash类型。
 */
public class UvCountApp {
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

        // 3.开1小时窗口，对用户去重
        SingleOutputStreamOperator<UvCount> result = inputDS.timeWindowAll(Time.hours(1))
                .apply(new UvAllWindowFunc());

        // 4.打印
        result.print();

        // 5.执行
        env.execute();
    }

    public static class UvAllWindowFunc implements AllWindowFunction<UserBehavior, UvCount, TimeWindow>{


        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> out) throws Exception {
            // 创建HashSet对数据进行去重
            HashSet<UserBehavior> hashSet = new HashSet<>();
            Iterator<UserBehavior> iterator = values.iterator();
            while (iterator.hasNext()){
                hashSet.add(iterator.next());
            }
            String windowEnd = new Timestamp(window.getEnd() * 1000L).toString();
            out.collect(new UvCount("uv",windowEnd,(long)hashSet.size()));

        }
    }
}
