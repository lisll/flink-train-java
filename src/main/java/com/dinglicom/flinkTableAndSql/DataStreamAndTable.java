package com.dinglicom.flinkTableAndSql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *
 * DataStream和Table的相互转换
 * 一： DataStream转换为Table有两种方式: 注册和转换。
 * 注意: DataStream转换为Table类型会自动推导。
 * 1,注册: 将DataStream注册成Catalog中的表，然后可以用Flink SQL对表中的数据进行查询和处理。
 * 2,转换: 将DataStream转换成Table对象，然后用Table API操作。
 * 二： Table转换为DataStream
 * Table转换为DataStream有两种模式：Append模式和Retract模式。
 * 可将Table转换为Row、Tuple、POJO、Case Class等数据类型的DataStream。
 * 1，Append模式: 当Table仅通过INSERT修改时使用此模式。
 * 2，Retract模式: 一般都可使用此模式。通过一个Boolean类型标记当前操作类型。True代表Add(添加)，False代表Retract(撤回)。
 *
 * @author ly
 * @Date Create in 14:32 2021/3/4 0004
 * @Description
 */
public class DataStreamAndTable {
    public static void main(String[] args) throws Exception {
//        transTable();
//        tableToStreamAppend();
        tableToStreamRetract();
    }

    //Retract模式: 一般都可使用此模式。通过一个Boolean类型标记当前操作类型。True代表Add(添加)，False代表Retract(撤回)。
    public static void tableToStreamRetract() throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // DataStream
        DataStreamSource<Tuple2<String, Integer>> stream = streamEnv.fromElements(new Tuple2<>("name1", 10), new Tuple2<>("name2", 20));
        Table table = tableEnv.fromDataStream(stream, "name,age");
        // Retract模式
        // Table转换为DataStream
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print();

        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream1 = tableEnv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.INT));
        tuple2DataStream1.print();
        streamEnv.execute();
    }


    //Append模式: 当Table仅通过INSERT修改时使用此模式。
    public static void tableToStreamAppend() throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // DataStream
        DataStreamSource<Tuple2<String, Integer>> stream = streamEnv.fromElements(new Tuple2<>("name1", 10), new Tuple2<>("name2", 20));

        Table table = tableEnv.fromDataStream(stream, "name,age");
        // Append模式
        // 转换为DataStream
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();
        // 转换为DataStream
        DataStream<Tuple> tupleDataStream = tableEnv.toAppendStream(table, Types.TUPLE(Types.STRING, Types.INT));
        tupleDataStream.print();
        streamEnv.execute();
    }


    //转换: 将DataStream转换成Table对象，然后用Table API操作。
    public static void transTable() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);
        //数据源
        DataStreamSource<Tuple2<String, Integer>> stream = senv.fromElements(new Tuple2<>("name1", 10), new Tuple2<>("name2", 20));
        // 将DataStream转换成Table对象,字段名为默认的f0,f1
        Table table = tableEnv.fromDataStream(stream);
        Table f1 = table.select("f1").where("f1>10");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(f1, Row.class);
//        rowDataStream.print();
        // 将DataStream转换为Table对象，使用自定义的字段名
        Table table1 = tableEnv.fromDataStream(stream, "name,age");
        Table name = table1.select("name").where("name='name2'");   // name2要使用引号引住，否则报错
        tableEnv.toAppendStream(name,Row.class).print();
        senv.execute();
    }

    //注册: 将DataStream注册成Catalog中的表，然后可以用Flink SQL对表中的数据进行查询和处理
    public static void regisTable() throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv);
        DataStreamSource<Tuple2<String, Integer>> tuple = senv.fromElements(new Tuple2<>("name1", 10), new Tuple2<>("name2", 20));
        String[] strings = tableEnv.listTables();
        // 将DataStream注册成Catalog中的表,表名table1,字段名为默认的f0,f1
        tableEnv.registerDataStream("table1",tuple);
        DataStream<Row> tableStream = tableEnv.toAppendStream(tableEnv.sqlQuery("select f1 from table1"), Row.class);
        tableStream.print();
        // 将DataStream注册成Catalog中的表,表名table2,字段名为name,age
        tableEnv.registerDataStream("table2",tuple,"name,age");
        tableEnv.toAppendStream(tableEnv.sqlQuery("select name from table2"), Row.class).print();
        senv.execute();
    }
}
