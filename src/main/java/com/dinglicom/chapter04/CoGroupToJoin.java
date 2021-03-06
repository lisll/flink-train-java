package com.dinglicom.chapter04;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Properties;

/**参考文章： https://wangpei.blog.csdn.net/article/details/103103805
 *
 * 用CoGroup实现Inner Join、Left Join、Right Join
 * left 流
 *{"userID": "user_2", "eventTime": "2019-11-16 17:30:01", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:02", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:05", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:10", "eventType": "browse", "productID": "product_1", "productPrice": 10}
 * right流
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:01", "eventType": "click", "pageID": "page_1"}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:02", "eventType": "click", "pageID": "page_1"}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:03", "eventType": "click", "pageID": "page_1"}
 * {"userID": "user_2", "eventTime": "2019-11-16 17:30:10", "eventType": "click", "pageID": "page_1"}
 * @Date Create in 14:44 2021/3/3 0003
 * @Description
 */
public class CoGroupToJoin {
    public static void main(String[] args) throws Exception {
        //1、解析命令行参数
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(fromArgs.getRequired("application.properties"));

        String kafkaBootstrapServers = parameterTool.getRequired("bootstrap.servers");

        String groupId = parameterTool.getRequired("group.id");

        //2、设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 并行度1，防止控制台输出乱序
        env.setParallelism(1);

        //3、添加Kafka数据源
        // 浏览流
        Properties browseProperties = new Properties();
        browseProperties.put("bootstrap.servers", kafkaBootstrapServers);
        browseProperties.put("group.id", groupId);

        // 浏览流
        String Browsetopic = "dblab02";
        FlinkKafkaConsumer<String> flinkKafkaConsumer1 = new FlinkKafkaConsumer<>(
                Browsetopic,
                new SimpleStringSchema(),
                browseProperties
        );
        SingleOutputStreamOperator<UserBrowseLog> browseSource = env.addSource(flinkKafkaConsumer1)
                .process(new BrowseKafkaProcessFunction())
                .assignTimestampsAndWatermarks(
                        new BrowseBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0L)));
        // 点击流
        String clicktopic = "dblab03";
        FlinkKafkaConsumer<String> flinkKafkaConsumer2 = new FlinkKafkaConsumer<>(
                clicktopic,
                new SimpleStringSchema(),
                browseProperties
        );
        SingleOutputStreamOperator<UserClickLog> clickSource = env.addSource(flinkKafkaConsumer2)
                .process(new ClickKafkaProcessFunction())
                .assignTimestampsAndWatermarks(
                        new ClickBoundedOutOfOrdernessTimestampExtractor(Time.seconds(0L)));
        browseSource.print();
        clickSource.print();


//        CoGroupedStreams<com.dinglicom.chapter04.UserBrowseLog, com.dinglicom.chapter04.UserClickLog>.Where<java.lang.String>.EqualTo equalTo = browseSource.coGroup(clickSource)
//                .where((KeySelector<UserBrowseLog, String>) userBrowseLog -> userBrowseLog.getProductID() + "_" + userBrowseLog.getEventTime())
//                .equalTo((KeySelector<UserClickLog, String>) userClickLog -> userClickLog.getPageID() + "_" + userClickLog.getEventTime());


        CoGroupedStreams.WithWindow<UserBrowseLog, UserClickLog, String, TimeWindow> window = browseSource.coGroup(clickSource)
                .where(new KeySelector<UserBrowseLog, String>() {
                    @Override
                    public String getKey(UserBrowseLog userBrowseLog) {
                        return userBrowseLog.getUserID() + "_" + userBrowseLog.getEventTime();
                    }
                })
                .equalTo(new KeySelector<UserClickLog, String>() {
                    @Override
                    public String getKey(UserClickLog userClickLog) throws Exception {
                        return userClickLog.getUserID() + "_" + userClickLog.getEventTime();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(10)));


        //WindowAssigner: TumblingEventTimeWindows
//        CoGroupedStreams.WithWindow<UserBrowseLog, UserClickLog, String, TimeWindow> window = equalTo1.window(TumblingEventTimeWindows.of(Time.seconds(20)));



        //Inner Join /Left Join /Right Join
//        window.apply(new InnerJoinFunction()).print();
//        window.apply(new LeftJoinFunction()).print();
//        window.apply(new RightJoinFunction()).print();

        window.apply(new JoinFunction()).print();

        env.execute();




    }


    //解析kafka数据
    private static class BrowseKafkaProcessFunction extends ProcessFunction<String, UserBrowseLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserBrowseLog> out) throws Exception {
            try {
                if (value != null && value.length() > 0) {
                    UserBrowseLog log = JSON.parseObject(value, UserBrowseLog.class);
                    if (log != null) {
                        out.collect(log);
                    }
                }
            } catch (Exception e) {
                System.err.println(e.toString());
            }
        }
    }

    //解析kafka数据
    private static class ClickKafkaProcessFunction extends ProcessFunction<String, UserClickLog> {
        @Override
        public void processElement(String value, Context ctx, Collector<UserClickLog> out) throws Exception {
            try {
                if (value != null && value.length() > 0) {
                    UserClickLog log = JSON.parseObject(value, UserClickLog.class);
                    if (log != null) {
                        out.collect(log);
                    }
                }
            } catch (Exception e) {
                System.err.println(e.toString());
            }
        }
    }



    private static class JoinFunction implements CoGroupFunction<UserBrowseLog, UserClickLog, String> {
        @Override
        public void coGroup(Iterable<UserBrowseLog> iterableBrowse, Iterable<UserClickLog> iterableClick, Collector<String> out) throws Exception {
            for (UserBrowseLog userBrowseLog : iterableBrowse) {
                System.out.println(userBrowseLog);
            }
            for(UserClickLog userClickLog:iterableClick){
                System.out.println(userClickLog);
            }
        }
    }

    /**
     * Inner Join
     * 获取每个用户每个时刻的浏览和点击。即浏览和点击都不为空才输出。
     */
    private static class InnerJoinFunction implements CoGroupFunction<UserBrowseLog, UserClickLog, String> {
        @Override
        public void coGroup(Iterable<UserBrowseLog> iterableBrowse, Iterable<UserClickLog> iterableClick, Collector<String> out) throws Exception {
            for (UserBrowseLog userBrowseLog : iterableBrowse) {
                for (UserClickLog userClickLog : iterableClick) {
                    out.collect(userBrowseLog + " ==Inner Join=> " + userClickLog);
                }
            }
        }
    }

    /**
     * Left Join
     * 获取每个用户每个时刻的浏览。有点击则顺带输出，没有则点击置空。
     */
    private static class LeftJoinFunction implements CoGroupFunction<UserBrowseLog, UserClickLog, String> {

        @Override
        public void coGroup(Iterable<UserBrowseLog> left, Iterable<UserClickLog> right, Collector<String> out) throws Exception {
            for (UserBrowseLog userBrowseLog : left) {
                boolean flag = true;
                for (UserClickLog userClickLog : right) {
                    flag = false;
                    out.collect(userBrowseLog + " ==Left Join=> " + userClickLog);
                }
                if (flag) {
                    out.collect(userBrowseLog + " ==Left Join=> " + "null");
                }
            }
        }
    }

    /**
     * Right Join
     * 获取每个用户每个时刻的点击。有浏览则顺带输出，没有则浏览置空。
     */
    private static class RightJoinFunction implements CoGroupFunction<UserBrowseLog, UserClickLog, String> {

        @Override
        public void coGroup(Iterable<UserBrowseLog> left, Iterable<UserClickLog> right, Collector<String> out) throws Exception {
            for (UserClickLog userClickLog : right) {
                boolean noElements = true;
                for (UserBrowseLog userBrowseLog : left) {
                    noElements = false;
                    out.collect(userBrowseLog + " ==Right Join=> " + userClickLog);
                }
                if (noElements) {
                    out.collect("null" + " ==Right Join=> " + userClickLog);
                }
            }
        }
    }
        // 提取时间戳生成水印
    private static class BrowseBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserBrowseLog>{

        public BrowseBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }
        @Override
        public long extractTimestamp(UserBrowseLog element) {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
            return dateTime.getMillis();
        }
    }


        private static class ClickBoundedOutOfOrdernessTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserClickLog> {

            public ClickBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
                super(maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(UserClickLog element) {
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                DateTime dateTime = DateTime.parse(element.getEventTime(), dateTimeFormatter);
                return dateTime.getMillis();
            }
        }
}
