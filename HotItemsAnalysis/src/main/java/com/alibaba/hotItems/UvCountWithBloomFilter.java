package com.alibaba.hotItems;

import com.alibaba.bean.UserBehavior;
import com.alibaba.bean.UvCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.sql.Timestamp;

/**
 * @author chenhuiup
 * @create 2020-11-27 15:12
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
      节省空间； 1亿用户 * 1个hash算法 * 10倍 = 10^9 bit = 10^6 * 1000 bit= 10^6 * 125B = 125 * 10^6 B = 125 * 10^3 KB = 125MB
    2）自定义Bloom过滤器，定义Bloom过滤器的长度，实现一个hash算法计算用户在数组中的offset，之后用offset在redis进行BitMap操作
    3）使用process API进行全窗口的操作，但是自定义触发器，每来一条数据就计算一次，并发送数据；
    4）自定义全窗口processFunction，实现将用户写入到redis，并将UVcount保存在redis中。UvCount的保存按理说应该使用String类型，
        但是这里使用hash类型。
 */
public class UvCountWithBloomFilter {
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
                .trigger(new UvTrigger())
                .process(new UvCountProcessAllWindowFunc());

        // 4.打印
        result.print();

        // 5.执行任务
        env.execute();
    }
}

//自定义布隆过滤器
/*
实现思路：
1.hash函数：一般使用3个哈希函数，提高唯一性；
2.布隆过滤器的容量：容量越大，hash碰撞的可能性越低；容量=用户数 * 哈希函数的个数 * （3-10倍）
3.容量一般设置为2的整次幂
 */
class Bloom {
    private Long cap; //定义布隆过滤器的长度

    public Bloom() {
    }

    public Bloom(Long cap) {
        this.cap = cap;
    }

    public long hash(String value) {
        long result = 0;
        for (char c : value.toCharArray()) {
            result = result * 31 + c;
        }

        return result & (cap - 1);
    }
}

/*
自定义触发器：定义数据什么时候发送和计算。1）计算；2）关窗。 可以分开定义。
1）TriggerResult.CONTINU：No action is taken on the window.（什么都不做）
2）FIRE_AND_PURGE：evaluates the window function and emits the window（计算数据，并发送数据，清空状态）
3）FIRE：the window is evaluated and results are emitted.The window is not purged, though, all elements are retained.（只计算数据）
4）PURGE：All elements in the window are cleared and the window is discarded,
    without evaluating the window function or emitting any elements.（发送数据，并清空状态）
 */
class UvTrigger extends Trigger<UserBehavior, TimeWindow> {

    @Override
    public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        //做收尾工作，比如关闭连接
    }
}

class UvCountProcessAllWindowFunc extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {
    private Jedis jedis; //创建redis连接
    private Bloom bloom; //创建布隆过滤器

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("hadoop102", 6379);
        bloom = new Bloom(1L << 29); //64M
    }

    @Override  //定义了触发器，每来一条数据，计算一条
    public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {
        //获取布隆过滤器的offset
        UserBehavior data = elements.iterator().next();
        long offset = bloom.hash(data.getUserId().toString());

        String windowEnd = new Timestamp(context.window().getEnd()).toString();
        // 定义redis存用户数的key，这里使用hash结果存储
        String redisCountKey = "user" + windowEnd;

        // 定义redis存用户的key，使用String类型
        String redisUserKey = "user" + windowEnd;

        // 如果用户不存在布隆过滤器，用户数+1，并修改布隆过滤器的值
        Boolean isExist = jedis.getbit(redisUserKey, offset);
        if (!isExist){
            jedis.hincrBy("UvCount",redisCountKey,1L); // 设置自增
            jedis.setbit(redisUserKey,offset,true); //修改布隆过滤器
        }
        Long count = Long.parseLong(jedis.hget("UvCount", redisCountKey));
        out.collect(new UvCount("uv",windowEnd,count));
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}