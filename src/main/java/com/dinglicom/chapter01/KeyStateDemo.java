package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 简单使用 ValueState和MapState
 * @author ly
 * @Date Create in 14:15 2021/2/7 0007
 * @Description
 */
public class KeyStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        senv.setParallelism(1);

        // 模拟数据源  测试ValueState
        DataStreamSource<Tuple2<Long, Long>> source = senv.fromElements(new Tuple2(2L, 3L), Tuple2.of(2L, 1L), Tuple2.of(2L, 5L), Tuple2.of(2L, 5L), Tuple2.of(2L, 10L), Tuple2.of(2L, 20L));
        // 首先得到 KeyedStream
        KeyedStream<Tuple2<Long, Long>, Tuple>
                keyStream = source.keyBy(0);

//        SingleOutputStreamOperator<Tuple2<Long, Integer>> flatmap = keyStream.map(new ValueStateDemo());
        SingleOutputStreamOperator<Tuple2<Long, Long>> flatmap = keyStream.flatMap(new ValueStateDemoFroRichFlat());

//        flatmap.print();


        StateTtlConfig stateTtlConfig = StateTtlConfig
                // 指定TTL时长为10S
                .newBuilder(Time.seconds(10))
                // 只对创建和写入操作有效
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                // 不返回过期的数据
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // 模拟数据源[userId,behavior,product]   测试MapState
        DataStreamSource<Tuple3<Long, String, String>> userBehaviors = senv.fromElements(
                Tuple3.of(1L, "buy", "iphone"),
                Tuple3.of(1L, "cart", "huawei"),
                Tuple3.of(1L, "buy", "logi"),
                Tuple3.of(1L, "fav", "oppo"),
                Tuple3.of(2L, "buy", "huawei"),
                Tuple3.of(2L, "buy", "onemore"),
                Tuple3.of(2L, "fav", "iphone"),
                Tuple3.of(2L, "fav", "iphone"),
                Tuple3.of(2L, "fav", "iphone"));

        DataStreamSink<Tuple3<Long, String, Integer>> print = userBehaviors.keyBy(0).flatMap(new MapStateDemo(stateTtlConfig)).print();





        senv.execute();
    }

    // 1，测试使用ValueState  使用RichMapFunction函数
    static class ValueStateDemo extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Integer>> {
        //初始化ValueState
        private transient ValueState<Tuple2<Long, Integer>> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Tuple2<Long, Integer>> valueStateDescriptor = new ValueStateDescriptor<>(
                    "valueState",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {
                    })
            );
            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public Tuple2<Long, Integer> map(Tuple2<Long, Long> tuple2) throws Exception {
            //取出状态值
            Tuple2<Long, Integer> currentValue = valueState.value();

            if (currentValue == null) {
                currentValue = new Tuple2<>(0L, 0);
            }
            // 更新状态值
            currentValue.f0 += 1;
            currentValue.f1 += tuple2.f1.intValue();
            valueState.update(currentValue);
            Tuple2<Long, Integer> out = null;
            // 如果当前累积的元素个数为2个则执行计算
            if (currentValue.f0 >= 2) {
                Long l = currentValue.f1 / currentValue.f0;
                out = new Tuple2(tuple2.f0, l.intValue());
                valueState.clear();
            }
            // 注意这个位置，这个位置out如果为null的情况下，会报错
            if (out == null) {
                out = new Tuple2<>(0L, 0);
            }
            return out;
        }
    }

    // 2，测试使用ValueState  使用RichFlatMapFunction函数
    static class ValueStateDemoFroRichFlat extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
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
//            valueStateDescriptor.setQueryable("");
        }

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            // 获取当前的状态值
            Tuple2<Long, Long> currentValue = sum.value();

            // 初始化状态值
            if (currentValue == null) {
                currentValue = Tuple2.of(0L, 0L);
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
    }


    /**
     * 统计每个用户每种行为的个数
     * 测试使用MapState
     */
    static class MapStateDemo extends RichFlatMapFunction<Tuple3<Long, String, String>, Tuple3<Long, String, Integer>> {
        //定义一个MapState句柄
        private transient MapState<String, Integer> behaviorCntState;

        private StateTtlConfig stateTtlConfig = null;
        public MapStateDemo(StateTtlConfig stateTtlConfig){
            this.stateTtlConfig = stateTtlConfig;
        }

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>(
                    "userBehavior",  // 状态描述符的名称
                    TypeInformation.of(new TypeHint<String>() {
                    }), // MapState状态的key的数据类型
                    TypeInformation.of(new TypeHint<Integer>() {
                    }) // MapState状态的value的数据类型
            );

            // 设置stateTtlConfig
            mapStateDescriptor.enableTimeToLive(stateTtlConfig);
            behaviorCntState = getRuntimeContext().getMapState(mapStateDescriptor); // 获取状态
        }

        @Override
        public void flatMap(Tuple3<Long, String, String> in, Collector<Tuple3<Long, String, Integer>> collector) throws Exception {
            Integer flag = 1;
            // 如果当前状态包括该行为，则+1
            if (behaviorCntState.contains(in.f1)) {
                flag = behaviorCntState.get(in.f1) + 1;
            }
            //更新状态
            behaviorCntState.put(in.f1, flag);
            collector.collect(Tuple3.of(in.f0, in.f1, flag));
        }
    }

}
