package com.dinglicom.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

/**
 * @author ly
 * @Date Create in 16:33 2021/3/11 0011
 * @Description
 */
public class OperatorDemo2 {

    private static class UserBehaviorCnt implements CheckpointedFunction, FlatMapFunction<Tuple3<Long, String, String>, Tuple3<Long, Long, Long>> {
        // 统计每个operator实例的用户行为数量的本地变量
        private Long opUserBehaviorCnt = 0L;
        // 每个key的state,存储key对应的相关状态
        private ValueState<Long> keyedCntState;
        // 定义operator state，存储算子的状态
        private ListState<Long> opCntState;

        @Override
        public void flatMap(Tuple3<Long, String, String> value, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
            if (value.f1.equals("buy")) {
                // 更新算子状态本地变量值
                opUserBehaviorCnt += 1;
                Long keyedCount = keyedCntState.value();
                // 更新keyedstate的状态 ,判断状态是否为null，否则空指针异常
                keyedCntState.update(keyedCount == null ? 1L : keyedCount + 1 );
                // 结果输出
                out.collect(Tuple3.of(value.f0, keyedCntState.value(), opUserBehaviorCnt));
            }
        }
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 使用opUserBehaviorCnt本地变量更新operator state
            opCntState.clear();
            opCntState.add(opUserBehaviorCnt);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            // 通过KeyedStateStore,定义keyedState的StateDescriptor描述符
            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor("keyedCnt", TypeInformation.of(new TypeHint<Long>() {
            }));

            // 通过OperatorStateStore,定义OperatorState的StateDescriptor描述符
            ListStateDescriptor opStateDescriptor = new ListStateDescriptor("opCnt", TypeInformation.of(new TypeHint<Long>() {
            }));
            // 初始化keyed state状态值
            keyedCntState = context.getKeyedStateStore().getState(valueStateDescriptor);
            // 初始化operator state状态
            opCntState = context.getOperatorStateStore().getListState(opStateDescriptor);
            // 初始化本地变量operator state
            for (Long state : opCntState.get()) {
                opUserBehaviorCnt += state;
            }
        }
    }
}
