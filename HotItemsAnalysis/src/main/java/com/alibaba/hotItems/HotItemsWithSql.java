package com.alibaba.hotItems;

import com.alibaba.bean.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * @author chenhuiup
 * @create 2020-11-25 18:36
 */
/*
使用Flink SQL实现：统计最近一小时热门商品TopN，每5分钟输出一次
 */
public class HotItemsWithSql {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        // 2.定义schema从文件系统中读取数据
        URL resource = HotItemsWithSql.class.getResource("/UserBehavior.csv");
//        tableEnv.connect(new FileSystem().path(resource.getPath()))
//                .withFormat(new OldCsv())
//                .withSchema(new Schema()
//                .field("userId", DataTypes.BIGINT())
//                .field("itemId",DataTypes.BIGINT())
//                .field("categoryId",DataTypes.SMALLINT())
//                .field("behavior",DataTypes.STRING())
//                .field("timestamp",DataTypes.BIGINT()))
//                .createTemporaryTable("userBehavior");
        SingleOutputStreamOperator<UserBehavior> inputDS = env.readTextFile(resource.getPath()).map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Integer.parseInt(split[2]),
                    split[3],
                    Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4.从流中创建表
        tableEnv.createTemporaryView("userBehavior",inputDS,"userId,itemId,categoryId,behavior,timestamp,timestamp.rowtime as rt");


        // 3.SQL实现，滑动窗口
        Table table = tableEnv.sqlQuery("select\n" +
                " *\n" +
                "from (\n" +
                " select\n" +
                "   *,\n" +
                "   row_number() over(partition by windowEnd order by cnt desc) as rk\n" +
                " from (\n" +
                "   select\n" +
                "     itemId,\n" +
                "     count(itemId) cnt,\n" +
                "     HOP_end(rt,interval '5' minute ,interval '1' hour) as windowEnd\n" +
                "   from userBehavior\n" +
                "   where behavior = 'pv'\n" +
                "   group by\n" +
                "     itemId,\n" +
                "     HOP(rt,interval '5' minute ,interval '1' hour)\n" +
                "   )t1\n" +
                " ) t2\n" +
                "where rk <= 5");

        // 4.打印
        tableEnv.toRetractStream(table, Row.class).print("table");

        // 5.执行任务
        env.execute();
    }
}
