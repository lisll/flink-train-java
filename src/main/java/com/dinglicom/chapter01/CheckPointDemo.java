package com.dinglicom.chapter01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ly
 * @Date Create in 17:16 2021/2/8 0008
 * @Description
 */
public class CheckPointDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
//        senv.addSource(
//                new FlinkKafkaConsumer<>("foo", new KafkaEventSchema(), properties)
//                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())).name("Example Source")
//                .keyBy("word")
//                .map(new MapFunction<KafkaEvent, KafkaEvent>() {
//                    @Override
//                    public KafkaEvent map(KafkaEvent value) throws Exception {
//                        value.setFrequency(value.getFrequency() + 1);
//                        return value;
//                    }
//                });
    }
}
