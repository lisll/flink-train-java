package com.dinglicom.queryableState;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;


/**Client端
 * @author ly
 * @Date Create in 11:23 2021/2/18 0018
 * @Description
 */
public class QueryClient {
    public static void main(String[] args) throws Exception {
//        final JobID jobId = JobID.fromHexString(args[0]);
//        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
//        final int jobManagerPort = args.length > 1 ? Integer.parseInt(args[1]) : 9069;
        // id,host,port 三个值都不能写错，否则会报错
        final JobID jobId = JobID.fromHexString("5b914af88e64ec685aaa82d9d5803ada");
        final String jobManagerHost = "localhost";
        final int jobManagerPort = 9069;


                QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);
        /*
      状态描述符
      event_five_seconds_max_pv 状态名称，Server端此名称是通过UUID生成的，这里随意即可
      TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})) 状态中值的类型，应和Server端相同
     */
        ValueStateDescriptor<Tuple2<String, Long>> stateDescriptor =
                new ValueStateDescriptor<>(
                        "xxx",    // 状态名称：client端可以随意写，并不一定要和服务端保持一致
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

//        final String key = "click";   // 这个key就是服务端的key，不能写错，否者找不到状态

        String[] keys = new String[]{"a","b","c","buy"};

    /*
     轮询执行查询
     event_five_seconds_max_pv queryableStateName，应和Server端的queryableStateName相同
    */
        while (true) {
            for(String key: keys){
                CompletableFuture<ValueState<Tuple2<String, Long>>> completableFuture =
                        client.getKvState(
                                jobId,
                                "lastFiveSecondsMaxPV",
                                key,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                stateDescriptor);

                System.out.println(completableFuture.get().value());
                Thread.sleep(1000);
            }
        }
    }
}
