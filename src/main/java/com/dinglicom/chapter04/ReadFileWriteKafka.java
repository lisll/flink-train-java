package com.dinglicom.chapter04;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

/**从本地文件读取数据，然后写入到kafka中
 * @author ly
 * @Date Create in 17:17 2021/2/26 0026
 * @Description
 */
public class ReadFileWriteKafka {


    public static void main(String[] args) throws Exception {
//        writeLocalFile("S:\\develop\\idea\\workspace_idea\\flink\\data\\localFile\\computer.log","12345我们都有一个加test");

        String localPath = "S:\\develop\\idea\\workspace_idea\\flink\\data\\localFile\\computer.log";

        // 模拟收集日志的过程
//        new Thread(new FileUtil()).start();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
//        DataStreamSource<String> fileSource = senv.readTextFile(localPath);
        // 读取不断变化的日志目录
        DataStreamSource<String> fileSource = senv.readFile(new TextInputFormat(new Path()), localPath, FileProcessingMode.PROCESS_CONTINUOUSLY, 12*1000, Types.STRING);
        SingleOutputStreamOperator<Computer> operator = fileSource.flatMap(new RichFlatMapFunction<String, Computer>() {
            @Override
            public void flatMap(String value, Collector<Computer> collector) throws Exception {
                String[] split_computer = value.split("\\|");
                //构建Computer对象
                if(split_computer.length>10){
                    String os_name = split_computer[0];
                    String os_arch = split_computer[1];
                    String os_version = split_computer[2];
                    String total_cup = split_computer[3];
                    String cup_ratio = split_computer[4];
                    String total_memory = split_computer[5];
                    String memory_ratio = split_computer[6];
                    String java_version = split_computer[7];
                    String java_vm_vendo = split_computer[8];
                    String eventTime = split_computer[9];
                    String reservedTime = split_computer[10];

                    Computer computer = new Computer.Builder(os_name, Integer.parseInt(total_cup), cup_ratio, total_memory, memory_ratio)
                            .setEventTime(Long.parseLong(eventTime))
                            .setReservedTime(reservedTime)
                            .build();
                    collector.collect(computer);
                }
            }
        });
//        operator.print();
        operator.global().addSink(writeToKafka());

        senv.execute();
    }


    public static FlinkKafkaProducer<Computer> writeToKafka() throws Exception {

        // 读取配置文件
        Config load = ConfigFactory.load();
        String bootstrap_servers_config = load.getString("bootstrap.servers");
        String group_id = load.getString("group.id");

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers_config);
        props.setProperty("group.id",group_id);

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


        String topic = "dblab04";

        // 写入的数据类型必须是string类型，此时kafka作为sink存在
//        FlinkKafkaProducer StringProducer = new FlinkKafkaProducer(
//                topic,
//                new LatestSimpleStringSchema(topic),
//                props,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        FlinkKafkaProducer pojoProducer = new FlinkKafkaProducer(
                topic,
                new PojoSchema(topic),
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );


        // FlinkKafkaProducer 的构造器中带有自定义分区器的方法都显示是过时
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(
                topic,
                new MySchema(),
                props,
                Optional.of(new CustomFlinkKafkaPartitioner())
        );

        new FlinkKafkaProducer(
                "192.168.142.128:9092",
                topic,
                new SimpleStringSchema()
    );


        return flinkKafkaProducer;
    }

    // 1，自定义发送字符串消息的sink
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


    private static class PojoSchema implements KafkaSerializationSchema<Computer>{
         private String topic;
         private ObjectMapper objectMapper;

        public PojoSchema(String topic) {

            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Computer computer, @Nullable Long aLong) {
            byte[] bytes = null;
            if(objectMapper==null){
                objectMapper = new ObjectMapper();
            }
            try {
                bytes = objectMapper.writeValueAsBytes(computer);
            } catch (JsonProcessingException e) {
                // 注意，在生产环境这是个非常危险的操作，
                // 过多的错误打印会严重影响系统性能，请根据生产环境情况做调整
                e.printStackTrace();
            }
            return new ProducerRecord<>(topic,bytes);
        }
    }

        //2，发送对象消息的sink
    class ObjSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {

        private String topic;
        private ObjectMapper mapper;

        public ObjSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> stringIntegerTuple2, @Nullable Long timestamp) {
            byte[] b = null;
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                b= mapper.writeValueAsBytes(stringIntegerTuple2);
            } catch (JsonProcessingException e) {
                // 注意，在生产环境这是个非常危险的操作，
                // 过多的错误打印会严重影响系统性能，请根据生产环境情况做调整
                e.printStackTrace();
            }
            return new ProducerRecord<byte[], byte[]>(topic, b);
        }
    }

  // 通过自定义分区器将数据发到固定的分区上
    private static class MyPartitioner extends FlinkKafkaPartitioner{

        @Override
        public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {

            return 1;  //  Math.abs获取绝对值
        }
    }

    private static class CustomFlinkKafkaPartitioner extends FlinkKafkaPartitioner{

        /**
         * @Description :
         * @Param:
         * record  记录
         * key KeyedSerializationSchema 中配置的key
         * value KeyedSerializationSchema中配置的value
         *
         */
        @Override
        public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            int hashCode = new String(key).hashCode();
            System.out.println("key: "+new String(key));
            System.out.println("value: "+new String(value));
//            System.out.println(hashCode);
            System.out.println(Math.abs(hashCode%partitions.length));
            return Math.abs(hashCode%partitions.length);  //  Math.abs获取绝对值
        }
    }


    public static class MySchema implements KeyedSerializationSchema {

        //element: 具体数据

        /**
         * 要发送的key
         *
         * @param element 原数据
         * @return key.getBytes
         */
        @Override
        public byte[] serializeKey(Object element) {
            //这里可以随便取你想要的key，然后下面分区器就根据这个key去决定发送到kafka哪个分区中，
            //element就是flink流中的真实数据，取出key后要转成字节数组
            return element.toString().split("\u0000")[0].getBytes();
        }

        /**
         * 要发送的value
         *
         * @param element 原数据
         * @return value.getBytes
         */
        @Override
        public byte[] serializeValue(Object element) {
            //要序列化的value，这里一般就原封不动的转成字节数组就行了
            return element.toString().getBytes();
        }

        @Override
        public String getTargetTopic(Object element) {
            //这里返回要发送的topic名字，没什么用，可以不做处理
            return null;
        }
    }





    //这种自定义序列化器的方式已经过期了
    private static class CustomerSchema implements KeyedSerializationSchema<Computer> {

    @Override
    public byte[] serializeKey(Computer computer) {
        return new byte[0];
    }

    @Override
    public byte[] serializeValue(Computer computer) {
        return computer.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getTargetTopic(Computer computer) {
        return null;
    }
}
}
