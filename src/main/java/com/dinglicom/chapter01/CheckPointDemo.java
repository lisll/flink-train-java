package com.dinglicom.chapter01;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ly
 * @Date Create in 17:16 2021/2/8 0008
 * @Description
 */
@Internal
public class CheckPointDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        Integer integer = 10;
        if(senv!=null){

        }
        System.out.println(integer.hashCode());
        System.out.println("10".hashCode());

    }
    private void test0(){}

    public void test(){ }

    public final  void test1(){ }

    public static void test2(){ }

    public static final void test3(){ }

    public String s1;
    private String s2 = "fsfa";
    public static int a;
    public final int c =9;
    public static final int b=10;
    private static final int d=10;

}
