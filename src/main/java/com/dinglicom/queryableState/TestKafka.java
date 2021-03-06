package com.dinglicom.queryableState;

import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 从 kafka消费数据经过转换之后重新吸入到kafka
 * @author ly
 * @Date Create in 17:16 2021/2/20 0020
 * @Description
 */
public class TestKafka {
    public static void main(String[] args) throws Exception {
        readFromKafka();
//        System.out.println(">>>>>>>>>>>>>>>>>>>>>."+writeToKafka());

    }

    // 从kafka中读取数据
    public static void readFromKafka() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //3、Kafka事件流
        //从Kafka中获取事件数据
        //数据：某个用户在某个时刻浏览或点击了某个商品,如
        /**
         *  {"userID": "user_5", "eventTime": "2019-08-18 08:17:19", "eventType": "browse", "productID": 3}
         *  {"userID": "user_3", "eventTime": "2019-08-18 08:17:19", "eventType": "click", "productID": 1}
         *  {"userID": "user_2", "eventTime": "2019-08-18 08:17:20", "eventType": "click", "productID": 3}
         *  {"userID": "user_5", "eventTime": "2019-08-18 08:17:20", "eventType": "browse", "productID": 1}
         *  {"userID": "user_2", "eventTime": "2019-08-18 08:19:52", "eventType": "browse", "productID": 8}
         */
        Config load = ConfigFactory.load();
        String bootstrap_servers_config = load.getString("bootstrap.servers");
        String group_id = load.getString("group.id");

        Properties kafkaProperties = new Properties();
//        props.put("bootstrap.servers", "192.168.142.128:9092");
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers_config);
        kafkaProperties.setProperty("group.id",group_id);
//        props.put("group.id", "test-consumer-group");

        String fromKafkaTopic = "dblab02";

        //  SimpleStringSchema为默认的序列化
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(fromKafkaTopic,
                new SimpleStringSchema(), kafkaProperties);
        flinkKafkaConsumer.setStartFromLatest();

        SingleOutputStreamOperator kafkaSource = env.addSource(flinkKafkaConsumer).name("KafkaSource").uid("source-id-kafka-source");

        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> eventStream = kafkaSource.process(new ProcessFunction<String, Tuple4<String, String, String, Integer>>() {

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple4<String, String, String, Integer>> out) {
                try {
                    JSONObject obj = JSONObject.parseObject(value);
                    String userID = obj.getString("userID");
                    String eventTime = obj.getString("eventTime");
                    String eventType = obj.getString("eventType");
                    int productID = obj.getIntValue("productID");
                    out.collect(new Tuple4<>(userID, eventTime, eventType, productID));
                } catch (Exception ex) {
                    System.out.println(ex.toString());
                }
            }
        });
        // 1，直接输出
//        eventStream.print();

        // 2,经过转换后sink到kafka
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2StreamOperator = eventStream.flatMap(new RichFlatMapFunction<Tuple4<String, String, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple4<String, String, String, Integer> tuple4, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(tuple4.f0, 1));
            }
        });

        SingleOutputStreamOperator<String> map = tuple2StreamOperator.keyBy(0).sum(1).map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0 + " : " + tuple2.f1;
            }
        });

        map.addSink(writeToKafka());

        env.execute("Testkafka");
    }


    /**向kafka中sink数据
     *参考文章： https://jifei-yang.blog.csdn.net/article/details/105645790
     * 使用 fixedPartition 的方式将数据写入到kafka
     *  1、实现 exactly-once 语义的 kafka sink
     *  2、fixedPartition: 一个 kafka partition 对应一个 flinkkafkaproducer。
     *       配置该种方式时，flink kafka producer 并行度应该不小于写入的 kafka topic 分区数，否则会导致有些分区没有数据
     *  3、注意：使用此方法需要合理设置 sink 的并行度，不能超过 topic 的分区数量 ，sink并发度 >= partition分区数
     *感觉2和3说的是同一件事情
     * @return
     * @throws Exception
     */
    public static FlinkKafkaProducer<String> writeToKafka() throws Exception {
//     final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


     // 读取配置文件
        Config load = ConfigFactory.load();
        String bootstrap_servers_config = load.getString("bootstrap.servers");
        String group_id = load.getString("group.id");

        Properties props = new Properties();
//        props.put("bootstrap.servers", "192.168.142.128:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers_config);
        props.setProperty("group.id",group_id);
//        props.put("group.id", "test-consumer-group");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

         // 一些其他设置
        // 增大输出数据量限制
//        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"");
//        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"");
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
//        props.setProperty("transaction.timeout.ms",1000 * 60 * 12 + "");
        // produce ack =-1 ，保证不丢
        props.setProperty(ProducerConfig.ACKS_CONFIG, -1 + "");
        // 开启 exactly-once 时必须设置幂等
        props.setProperty("enable.idempotence","true");
        // 设置了retries参数，可以在Kafka的Partition发生leader切换时，Flink不重启，而是做5次尝试：
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        // 开启 RETREIS 时，可能导致消息乱序，如果要求消息严格有序，配置 MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION 为 1
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1");


        String topic = "dblab03";

        // 这种构造方式已经过期了
        new FlinkKafkaProducer(
              topic,
                new SimpleStringSchema(),
                props
        );

        // 写入的数据类型必须是string类型，此时kafka作为sink存在
       return new FlinkKafkaProducer(
                topic,
               new LatestSimpleStringSchema(topic),
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


    }

    static class LatestSimpleStringSchema implements KafkaSerializationSchema<String> {

        private static final long serialVersionUID = 1221534846982366764L;

        private String topic;

        public LatestSimpleStringSchema(String topic) {
            super();
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(String message, Long timestamp) {
            return new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8));
        }
    }
}
