package example.java.state.p5_checkpointed;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/14
 **/
public class BufferingSink implements SinkFunction<Tuple2<String,Integer>>,CheckpointedFunction {

    // 每当执行检查点的时候都会调用  snapshotstate()
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    // 首次初始化用户定义的函数或者冲检查点恢复时，都会调用initializeState
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
