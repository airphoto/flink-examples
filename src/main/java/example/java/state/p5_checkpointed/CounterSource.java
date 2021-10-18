package example.java.state.p5_checkpointed;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @author lihuasong
 * @description
 *      与其他算子相比，有状态的来源需要更多的关注。
 *      为了使状态和输出集合的更新成为原子（在故障/恢复时精确一次的语义所需）
 *     ，用户需要从源的上下文中获取锁定。
 * @create 2020/4/16
 **/
public class CounterSource extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    private Long offset;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        final Object lock = sourceContext.getCheckpointLock();

        while (running){
            synchronized (lock){
                sourceContext.collect(offset);
                offset += 1;
            }
        }
    }


    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public List<Long> snapshotState(long l, long l1) throws Exception {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> list) throws Exception {
        for (Long aLong : list) {
            offset = aLong;
        }
    }



}
