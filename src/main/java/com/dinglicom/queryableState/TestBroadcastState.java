package com.dinglicom.queryableState;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * https://wangpei.blog.csdn.net/article/details/99698978
 *
 * Flink DataStream基于Broadcast State动态更新配置以实现实时过滤数据并增加字段
 *
 * 广播状态可以用来解决如下问题:
 * 一条流需要根据规则或配置处理数据，而规则或配置又是随时变化的。此时，就可将规则或配置作为广播流广播出去，
 * 并以Broadcast State的形式存储在下游Task中。下游Task根据Broadcast State中的规则或配置来处理常规流中的数据。
 * @author ly
 * @Date Create in 16:05 2021/2/20 0020
 * @Description
 */
public class TestBroadcastState {
    public static void main(String[] args) throws Exception {
        //2、配置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //设置StateBackend
//        env.setStateBackend((StateBackend) new FsStateBackend("S:\\develop\\idea\\workspace_idea\\flink\\data\\FsStateBackend", true));
        //设置Checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(10 * 1000);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //3、Kafka事件流
        //从Kafka中获取事件数据
        //数据：某个用户在某个时刻浏览或点击了某个商品,如
        //{"userID": "user_3", "eventTime": "2019-08-17 12:19:47", "eventType": "browse", "productID": 1}
        /**
         *
         */
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "192.168.142.128:9092");
        kafkaProperties.put("group.id", "test-consumer-group");

        String fromKafkaTopic = "dblab02";

        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(fromKafkaTopic, new SimpleStringSchema(), kafkaProperties);
        flinkKafkaConsumer.setStartFromLatest();
        SingleOutputStreamOperator kafkaSource = env.addSource(flinkKafkaConsumer).name("KafkaSource").uid("source-id-kafka-source");

        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> eventStream = kafkaSource.process(new ProcessFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple4<String, String, String, Integer>> out) {
                try {
                    if(null != null && value.length()>0){
                        JSONObject obj = JSONObject.parseObject(value);
                        String userID = obj.getString("userID");
                        String eventTime = obj.getString("eventTime");
                        String eventType = obj.getString("eventType");
                        int productID = obj.getIntValue("productID");
                        out.collect(new Tuple4<>(userID, eventTime, eventType, productID));
                    }
                } catch (Exception ex) {
                    System.out.println("value值为 : "+value);
                    System.out.println(ex.toString());
                }
            }
        });

        //4、Mysql配置流
        //自定义Mysql Source，周期性地从Mysql中获取配置，并广播出去
        //数据: 用户ID,用户姓名，用户年龄
        DataStreamSource<HashMap<String, Tuple2<String, Integer>>> mysqlConfigStream = env.addSource(
                new MysqlSource("127.0.0.1", 3306, "springboot", "root", "root"));


        /*
          (1) 先建立MapStateDescriptor
          MapStateDescriptor定义了状态的名称、Key和Value的类型。
          这里，MapStateDescriptor中，key是Void类型，value是Map<String, Tuple2<String,Int>>类型。
         */
        MapStateDescriptor<Void, Map<String, Tuple2<String,Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
/*
          (2) 将配置流广播，形成BroadcastStream
         */
        BroadcastStream<HashMap<String, Tuple2<String, Integer>>> broadcastConfigStream = mysqlConfigStream.broadcast(configDescriptor);

        //5、事件流和广播的配置流连接，形成BroadcastConnectedStream
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, HashMap<String, Tuple2<String, Integer>>> connectedStream = eventStream.connect(broadcastConfigStream);

       //6、对BroadcastConnectedStream应用process方法，根据配置(规则)处理事件
        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> resultStream = connectedStream.process(new CustomBroadcastProcessFunction());

        //7、输出结果
        resultStream.print();

        //8、生成JobGraph，并开始执行
        env.execute();

    }

    /**
     * 自定义BroadcastProcessFunction
     * 当事件流中的用户ID在配置中出现时，才对该事件处理, 并在事件中补全用户的基础信息
     * Tuple4<String, String, String, Integer>: 第一个流(事件流)的数据类型
     * HashMap<String, Tuple2<String, Integer>>: 第二个流(配置流)的数据类型
     * Tuple6<String, String, String, Integer,String, Integer>: 返回的数据类型
     */
    static class CustomBroadcastProcessFunction extends BroadcastProcessFunction<Tuple4<String, String, String, Integer>, HashMap<String, Tuple2<String, Integer>>, Tuple6<String, String, String, Integer, String, Integer>> {

        /**定义MapStateDescriptor*/
        MapStateDescriptor<Void, Map<String, Tuple2<String,Integer>>> configDescriptor = new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        /**
         * 读取状态，并基于状态，处理事件流中的数据
         * 在这里，从上下文中获取状态，基于获取的状态，对事件流中的数据进行处理
         * @param value 事件流中的数据
         * @param ctx 上下文
         * @param out 输出零条或多条数据
         * @throws Exception
         */
        @Override
        public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {

            //事件流中的用户ID
            String userID = value.f0;

            //获取状态
            ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(configDescriptor);
            Map<String, Tuple2<String, Integer>> broadcastStateUserInfo = broadcastState.get(null);

            //配置中有此用户，则在该事件中添加用户的userName、userAge字段。
            //配置中没有此用户，则丢弃，所以mysqy中的配置表起到一个过滤的作用，（kafka的输出和mysql中的数据类似于在进行innerjoin）

            Tuple2<String, Integer> userInfo = broadcastStateUserInfo.get(userID);
            if(userInfo!=null){
                out.collect(new Tuple6<>(value.f0,value.f1,value.f2,value.f3,userInfo.f0,userInfo.f1));
            }

        }

        /**
         * 处理广播流中的每一条数据，并更新状态
         * @param value 广播流中的数据
         * @param ctx 上下文
         * @param out 输出零条或多条数据
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(HashMap<String, Tuple2<String, Integer>> value, Context ctx, Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {

            //获取状态
            BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(configDescriptor);

            //清空状态
            broadcastState.clear();

            //更新状态
            broadcastState.put(null,value);

        }
    }

    /**
     * 自定义Mysql Source，每隔 secondInterval 秒从Mysql中获取一次配置
     */
    static class MysqlSource extends RichSourceFunction<HashMap<String, Tuple2<String, Integer>>> {

        private String host;
        private Integer port;
        private String db;
        private String user;
        private String passwd;

        private volatile boolean isRunning = true;

        private Connection connection;
        private PreparedStatement preparedStatement;

        MysqlSource(String host, Integer port, String db, String user, String passwd) {
            this.host = host;
            this.port = port;
            this.db = db;
            this.user = user;
            this.passwd = passwd;
        }

        /**
         * 开始时, 在open()方法中建立连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.jdbc.Driver");
            connection = (Connection) DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=UTF-8", user, passwd);
            String sql = "select userID,userName,userAge from user_info";
            preparedStatement = connection.prepareStatement(sql);
        }

        /**
         * 执行完，调用close()方法关系连接，释放资源
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();

            if (connection != null) {
                connection.close();
            }

            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        /**
         * 调用run()方法获取数据
         *
         * @param ctx
         */
        @Override
        public void run(SourceContext<HashMap<String, Tuple2<String, Integer>>> ctx) {
            try {
                while (isRunning) {
                    HashMap<String, Tuple2<String, Integer>> output = new HashMap<>();
                    ResultSet resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        String userID = resultSet.getString("userID");
                        String userName = resultSet.getString("userName");
                        int userAge = resultSet.getInt("userAge");
                        output.put(userID, new Tuple2<>(userName, userAge));
                    }
                    ctx.collect(output);
                    //每隔多少秒执行一次查询
                    Thread.sleep(3000 );
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }

        /**
         * 取消时，会调用此方法
         */
        @Override
        public void cancel() {
            isRunning = false;
        }

    }
}
