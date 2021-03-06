package com.dinglicom.queryableState;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/**
 * @author ly
 * @Date Create in 16:11 2021/2/18 0018
 * @Description
 */
public class QueryClient2 {
    public static void main(String[] args) throws Exception {
        final JobID jobId = JobID.fromHexString("56450d8904e94cfd950d10a6346ede01");
        final String jobManagerHost = "localhost";
        final int jobManagerPort =  9069;

        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);

        ValueStateDescriptor<Tuple5<String, String, String, Long, Long>> vvvvv = new ValueStateDescriptor<>(
                "vvvvv",
                Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG)
        );

        String key = "d";
        /*
     轮询执行查询
     lastFiveSecondsCountSum queryableStateName，应和Server端的queryableStateName相同
    */
        while (true){
            CompletableFuture<ValueState<Tuple5<String, String, String, Long, Long>>> kvState = client.getKvState(
                    jobId,
                    "lastFiveSecondsCountSum",
                    key,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    vvvvv

            );

         Tuple5<String, String, String, Long, Long> value = kvState.get().value();
            System.out.println(
                    "Key: "
                            + value.f0
                            + " ,WindowStart: "
                            + value.f1
                            + " ,WindowEnd: "
                            + value.f2
                            + " ,Count: "
                            + value.f3
                            + " ,Sum: "
                            + value.f4);

            Thread.sleep(3000);
        }
    }
}
