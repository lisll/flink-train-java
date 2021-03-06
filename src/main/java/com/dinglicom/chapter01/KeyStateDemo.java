package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ly
 * @Date Create in 14:15 2021/2/7 0007
 * @Description
 */
public class KeyStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<Tuple2<Long, Long>> source = senv.fromElements(new Tuple2(2L, 1L), Tuple2.of(2L, 1L), Tuple2.of(2L, 5L),Tuple2.of(2L, 5L),Tuple2.of(2L, 10L),Tuple2.of(2L, 10L));
        // 首先得到 KeyedStream
        KeyedStream<Tuple2<Long, Long>, Tuple> keyStream = source.keyBy(0);
        SingleOutputStreamOperator<Tuple2<Long, Long>> flatmap = keyStream.flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {

            /**
             * ValueState状态句柄. 第一个值为count，第二个值为sum。
             */
            private transient ValueState<Tuple2<Long, Long>> sum;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "valueState",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        })
                );
                sum = getRuntimeContext().getState(valueStateDescriptor);
                valueStateDescriptor.setQueryable("");
            }

            @Override
            public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
                // 获取当前的状态值
                Tuple2<Long, Long> currentValue = sum.value();

                // 初始化状态值
                if(currentValue == null){
                    currentValue = Tuple2.of(0L,0L);
                }

                // 更新当前的状态值
                currentValue.f0 += 1;
                currentValue.f1 += input.f1;
                sum.update(currentValue);
                // 如果当前累积的元素个数为2个则执行计算
                if (currentValue.f0 >= 2) {
                    // 这个算法类似于每两个元素计算一次平均值
                    out.collect(new Tuple2<>(input.f0, currentValue.f1 / currentValue.f0));
                    sum.clear();
                }
            }
        });
        flatmap.print();
        senv.execute();
    }
}
