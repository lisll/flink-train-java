package com.dinglicom.chapter04;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 从kafka中读取数据，经过处理之后再重新写入到kafka， 实现PV统计
 * 启动一个kafka生产者，输入如下数据：
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:20", "eventType": "click", "productID": 3}
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:21", "eventType": "browse", "productID": 3}
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:22", "eventType": "browse", "productID": 3}
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:29", "eventType": "click", "productID": 3}
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:39", "eventType": "browse", "productID": 3}
 * {"userID": "user_5", "eventTime": "2019-08-18 09:17:49", "eventType": "browse", "productID": 3}
 * 得到的结果如下：
 * 2019-08-18 09:17:20,2019-08-18 09:17:40,browse,3
 * 2019-08-18 09:17:20,2019-08-18 09:17:40,click,2
 *
 * @Date Create in 9:42 2021/3/3 0003
 * @Description
 */
public class ReadWriteKafka {
    public static void main(String[] args) throws Exception {
        // 通过控制台传参的方式，将路径传入
        // 形式： --application.properties S:\develop\idea\workspace_idea\flink\flink-train-java\src\main\resources\application.properties
        ParameterTool tool = ParameterTool.fromArgs(args);
        String path = tool.getRequired("application.properties");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        // 获取配置文件中的参数

        //1，checkpoint参数
        String checkpointDirectory = parameterTool.getRequired("checkpointDirectory");
        long checkpointSecondInterval = parameterTool.getLong("checkpointSecondInterval");

        //Kafka参数（consumerKafka）
        String fromKafkaBootstrapServers = parameterTool.getRequired("bootstrap.servers");
        String fromKafkaGroupID = parameterTool.getRequired("group.id");
        String fromKafkaAutoOffsetReset= parameterTool.getRequired("auto.offset.reset");

        //toKafka参数（producerKafka)
        String toKafkaBootstrapServers = parameterTool.getRequired("bootstrap.servers");

        //窗口参数
        long tumblingWindowLength = parameterTool.getLong("tumblingWindowLength");
        long outOfOrdernessSeconds = parameterTool.getLong("outOfOrdernessSeconds");


        /**配置运行环境*/

        //设置Local Web Server
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT,8081);
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置水印生成间隔为100毫秒（默认为200毫秒）
        env.getConfig().setAutoWatermarkInterval(100);
        //设置StateBackend     true：表示开启异步快照
        env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));

        //设置Checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(checkpointSecondInterval*10);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
             /*
              *  ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：
              *     取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
              *  ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：
              *     取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
             */
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);



        /**配置数据源*/
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,fromKafkaBootstrapServers);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG,fromKafkaGroupID);
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,fromKafkaAutoOffsetReset);

       // 在Flink中, 默认支持动态发现Kafka中新增的Topic或Partition，但需要手动开启。
        //flink.partition-discovery.interval-millis: 检查间隔,单位毫秒。
        kafkaProperties.put("flink.partition-discovery.interval-millis","10000");

        // 采用正则的形式，一次读取多个Topic
        Pattern tmpeTopic = Pattern.compile("dblab0[1-9]{1}");
        System.out.println("tmpeTopic: "+tmpeTopic);
        Pattern topics = Pattern.compile("dblab03");
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(topics,new SimpleStringSchema(),kafkaProperties);

        // 采用动态提交offset的方式，实际生产中一般都是手动提交offset
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        SingleOutputStreamOperator kafkaSource = env.addSource(flinkKafkaConsumer).name("flinkSource").uid("source-id");


        // flatMap自带过滤的功能
        kafkaSource.flatMap(new RichFlatMapFunction<String,Tuple4<String, String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple4<String, String, String, Integer>> collector) throws Exception {
               try {
                   JSONObject obj = JSON.parseObject(value);
                   String userId = obj.getString("userID");
                   String eventTime = obj.getString("eventTime");
                   String evnetType = obj.getString("eventType");
                   int productID = obj.getInteger("productID");
                   collector.collect(Tuple4.of(userId,eventTime,evnetType,productID));
               } catch (Exception e) {
                   System.err.println(e.toString());
               }
            }
        });

        //抽取转换,入参：string，出参：Tuple4
        // 因为map函数是有返回值的，所以使用map处理之后，最好要过滤一下数据，比如空数据等
        SingleOutputStreamOperator mapSource = kafkaSource.map(new RichMapFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> map(String value) throws Exception {
                Tuple4<String, String, String, Integer> output = null;
                try {
                    JSONObject obj = JSON.parseObject(value);
                    output = new Tuple4<>();
                    output.f0 = obj.getString("userID");
                    output.f1 = obj.getString("eventTime");
                    output.f2 = obj.getString("eventType");
                    output.f3 = obj.getInteger("productID");
                } catch (Exception e) {
                    System.err.println(e.toString());
                }
               int a = 1/0;
                return output;
            }
        }).name("Map: ExtractTransform").uid("map-id");

        SingleOutputStreamOperator filterSource =mapSource.filter((FilterFunction<Tuple4<String, String, String, Integer>>) o -> null !=o).name("Filter: FilterExceptionData").uid("filter-id");


        /**抽取时间戳并发射水印*/
        SingleOutputStreamOperator timeSource = filterSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<String, String, String, Integer>>(Time.seconds(outOfOrdernessSeconds)) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long time = 0L;
            @Override
            public long extractTimestamp(Tuple4<String, String, String, Integer> element) {
                try {
                    Date parse = format.parse(element.f1);
                    time = parse.getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return time;
            }
        }).uid("watermark-id");

        /*
            窗口统计,滚动窗口
         * 并且采用增量聚合函数和全量窗口函数进行组合的方式进行窗口计算
         * */
        SingleOutputStreamOperator aggregateSource = timeSource.keyBy((KeySelector<Tuple4<String, String, String, Integer>, String>) value -> value.f2)
                .window(TumblingEventTimeWindows.of(Time.seconds(tumblingWindowLength)))

                //在每个窗口(Window)上应用WindowFunction(CustomWindowFunction)
                //CustomAggFunction用于增量聚合
                //在每个窗口上，先进行增量聚合(CustomAggFunction),然后将增量聚合的结果作为WindowFunction(CustomWindowFunction)的输入,计算后并输出
                //具体: 可参考底层AggregateApplyWindowFunction的实现
                .aggregate(new CustomerAggregateFunction(), new CustomerProcessWindowFunction());
       // 控制台打印
        aggregateSource.print();

        // 输出到kafka
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,toKafkaBootstrapServers);
        kafkaProducerProperties.setProperty("transaction.timeout.ms",60000+"");

        String topic= "dblab04";
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(
                topic,
                new LatestSimpleStringSchema(topic),
                kafkaProducerProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        aggregateSource.addSink(flinkKafkaProducer);
        env.execute();
    }

    /**自定义  AggregateFunction函数（增量聚合函数）
     * 增量聚合，这里实现累加效果
     * 三个泛型分别为： IN, ACC, OUT
     */
    private static class CustomerAggregateFunction implements AggregateFunction<Tuple4<String, String, String, Integer>,Long,Long>{

        /**
         * 初始化一个累加器
         * @return
         */
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        /**
         *
         * @param tuple4  输入数据源
         * @param accumulator  中间结果值
         * @return
         */
        @Override
        public Long add(Tuple4<String, String, String, Integer> tuple4, Long accumulator) {
            return accumulator+1;
        }

        /**
         * 从累加器中计算最终的结果并返回
         * @param aLong
         * @return
         */
        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        /**
         * 合并两个累加器并返回结果
         * @param acc1  中间结果值1
         * @param acc2  中间结果值2
         * @return
         */
        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1+acc2;
        }
    }

    /**
     * 全量窗口函数
     *四个泛型分别为：
     * IN  OUT KEY WINDOW
     */
    private static class CustomerProcessWindowFunction extends ProcessWindowFunction<Long,String,String,TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            Long windowPV = elements.iterator().next();
            String output=format.format(new Date(windowStart))+","+format.format(new Date(windowEnd))+","+key+","+windowPV;
            out.collect(output);
        }
    }

    // 自定义发送字符串消息的sink
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
