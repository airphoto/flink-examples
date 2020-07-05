package com.lhs.flink.rule.sink;

import com.lhs.flink.rule.pojo.RedisData;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @ClassNameRedisSink
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 10:07
 * @Version V1.0
 **/
public class RedisSink extends RichSinkFunction<RedisData> implements CheckpointedFunction {

    private Logger logger = LoggerFactory.getLogger(RedisSink.class);

    private  ParameterTool parameterTool;
    private transient ListState<RedisData> checkpointedState;
    private List<RedisData> bufferedElements;

    public RedisSink(){}

    public RedisSink(ParameterTool parameterTool){
        this.parameterTool = parameterTool;
    }

    /**
     * 初始化的时候执行一次
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RedisHelper.mkPool(parameterTool);
        bufferedElements = new ArrayList<>();
        logger.debug("sink init");
    }

    /**
     * 任务停止的时候执行一次
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        RedisHelper.destroy();
        logger.debug("sink close");
    }

    @Override
    public void invoke(RedisData value, Context context) throws Exception {
        bufferedElements.add(value);
        Jedis jedis = RedisHelper.getJedisPool().getResource();
        Pipeline pipeline = jedis.pipelined();
        RedisHelper.saveRedisData(pipeline,value);
        RedisHelper.returnSource(jedis, pipeline);
        bufferedElements.clear();
    }

    /**
     * 在checkpoint的时候回被调用，用于snapshot state 通常用于flush commit synchronize 外部系统
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        try {
            checkpointedState.clear();
            checkpointedState.addAll(bufferedElements);
            logger.debug("snapshot state completed");
        }catch (Exception e){
            logger.error("snapshot state error",e);
        }
    }

    /**
     * 在parallel function 初始化的时候（第一次初始化或者从前一次checkpoint recover的时候）被调用，通常用来初始化state，以及处理state recover的逻辑
     * @param functionInitializationContext
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        try {
            ListStateDescriptor<RedisData> descriptor = new ListStateDescriptor<RedisData>(
                    "buffered-elements",
                    TypeInformation.of(new TypeHint<RedisData>() {
                    })
            );

            checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

            if (functionInitializationContext.isRestored()) {
                bufferedElements.addAll((Collection<? extends RedisData>) checkpointedState.get());
            }
            logger.debug("initialize state completed");
        }catch (Exception e){
            logger.error("initialize state error",e);
        }
    }
}
