package com.dinglicom.chapter03;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 注意：
 *
 * Side-Output是从Flink 1.3.0开始提供的功能，支持了更灵活的多路输出。
 * Side-Output可以以侧流的形式，以不同于主流的数据类型，向下游输出指定条件的数据、异常数据、迟到数据等等。
 * Side-Output通过ProcessFunction将数据发送到侧路OutputTag
 *
 * @author ly
 * @Date Create in 10:22 2021/2/22 0022
 * @Description
 */
public class SplitStreamBySideOutput {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Tuple3<String, String, String>> eleSource = senv.fromElements(
                new Tuple3<>("productID1", "click", "user_1"),
                new Tuple3<>("productID1", "click", "user_2"),
                new Tuple3<>("productID1", "browse", "user_1"),
                new Tuple3<>("productID2", "browse", "user_1"),
                new Tuple3<>("productID2", "click", "user_2"),
                new Tuple3<>("productID2", "click", "user_1")
        );

        /**1、定义OutputTag*/  // 注意： 不加 {} 会报错
//        OutputTag<Tuple3<String, String, String>> outTag = new OutputTag<Tuple3<String, String, String>>("productID2");

        /**1、定义OutputTag*/    //这里其实在构造 OutputTag 的匿名子类，就是在初始化该对象，不知道为啥要这么写
        OutputTag<Tuple3<String, String, String>> sideOutputTag = new OutputTag<Tuple3<String, String, String>>("side-output-tag"){};

//         正常处理流
        SingleOutputStreamOperator<Tuple3<String, String, String>> process = eleSource.process(new ProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            @Override
            public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                if (value.f0.equals("productID2")) {
                   ctx.output(sideOutputTag, value);  //侧流-只输出特定数据
//                    out.collect(value);// 这么写是不对的，这种方式是无法获取侧输出流的
                } else {
                    out.collect(value);   // 输出主流
                }
            }
        });

//        process.print();    // 输出主流

        process.getSideOutput(sideOutputTag).print();    //输出侧流

//        senv.execute();
    }
}
