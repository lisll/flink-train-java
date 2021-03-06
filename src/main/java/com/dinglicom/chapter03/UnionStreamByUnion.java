package com.dinglicom.chapter03;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 合流(Union/Connect)
 * 1，基于Union  Union可以将两个或多个同数据类型的流合并成一个流。
 * 2，基于Connect
 *      2.1 Connect可以用来合并两种不同类型的流。
 *      2.2 Connect合并后，可用map中的CoMapFunction或flatMap中的CoFlatMapFunction来对合并流中的每个流进行处理。
 * @author ly
 * @Date Create in 11:27 2021/2/22 0022
 * @Description
 */
public class UnionStreamByUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /**输入数据源source1*/
        DataStreamSource<Tuple3<String, String, String>> source1 = senv.fromElements(new Tuple3<>("productID1", "click", "user_1"));
        DataStreamSource<Tuple3<String,String,String>> source2 = senv.fromElements(new Tuple3<>("1","1","1"));
        /**输入数据源source3*/
        DataStreamSource<Tuple3<String, String, String>> source3 = senv.fromElements(
                new Tuple3<>("productID2", "browse", "user_1"),
                new Tuple3<>("productID2", "click", "user_2"),
                new Tuple3<>("productID2", "click", "user_1")
        );
        //Union可以将两个或多个同数据类型的流合并成一个流。
        DataStream<Tuple3<String, String, String>> union = source1.union(source2,source3);

        union.print();


        DataStreamSource<String> stringSource = senv.fromElements("productID3:click:user_1", "productID3:browse:user_2");

        ConnectedStreams<Tuple3<String, String, String>, String> connect = union.connect(stringSource);

        /*   用CoMap处理合并后的流*/
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = connect.flatMap(new CoFlatMapFunction<Tuple3<String, String, String>, String, String>() {
            @Override
            public void flatMap1(Tuple3<String, String, String> value, Collector<String> out) throws Exception {
                // 定义第一个流的处理逻辑
                out.collect(value.f0 + "," + value.f1 + "," + value.f2);
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                // 定义第二个流的处理逻辑
                String[] split = value.split(":");
                out.collect(split[0] + ">>" + split[1]);
            }
        });

        stringSingleOutputStreamOperator.print();
        senv.execute();
    }
}
