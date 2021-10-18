package example.java.state.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/4/14
 **/
public class RedisSinkInstance2 extends RichSinkFunction<Tuple2<String,Long>> implements CheckpointedFunction{

    private Jedis jedis;

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close");
        this.jedis.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedis = new Jedis("10.51.237.173",16379);
        this.jedis.auth("XLhy!321YH");
        this.jedis.select(4);
        System.out.println("open"+jedis);
    }

    @Override
    public void invoke(Tuple2<String,Long> value, Context context) throws Exception {
        this.jedis.set(value.f0,value.f1.toString());
        System.out.println("invoke -> "+value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
