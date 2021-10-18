package com.lhs.flink.rule.sink;

import com.lhs.flink.rule.monitor.JsonGuigeMonitor;
import com.lhs.flink.rule.pojo.RedisDataWithName;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
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
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ClassNameRedisSink
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 10:07
 * @Version V1.0
 **/
public class RedisSinkWithPools extends RichSinkFunction<RedisDataWithName> implements CheckpointedFunction {

    private Logger logger = LoggerFactory.getLogger(RedisSinkWithPools.class);

    private  ParameterTool parameterTool;
    private transient ListState<RedisDataWithName> checkpointedState;
    private List<RedisDataWithName> bufferedElements;
    private transient JsonGuigeMonitor errorMonitor;
    private transient JsonGuigeMonitor dataRetainMonitor;

    public RedisSinkWithPools(){}

    public RedisSinkWithPools(ParameterTool parameterTool){
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
        try {
            bufferedElements = new ArrayList<>();

            ExpiringMap<String,Object> monitorMap = ExpiringMap
                    .builder()
                    .expiration(this.parameterTool.getLong("metric.map.ttl",300), TimeUnit.SECONDS)
                    .expirationPolicy(ExpirationPolicy.ACCESSED)
                    .build();

            Map<String,Object> dataRetainMap = new HashMap<>();
            dataRetainMap.put("data",bufferedElements.size());

            errorMonitor = getRuntimeContext().getMetricGroup().gauge("gauge_sink_error",new JsonGuigeMonitor(monitorMap));

            dataRetainMonitor = getRuntimeContext().getMetricGroup().gauge("gauge_data_retain",new JsonGuigeMonitor(dataRetainMap));

            logger.debug("sink init");
        }catch (Exception e){
            logger.error("jedis init error");
        }
    }

    /**
     * 任务停止的时候执行一次
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        logger.debug("sink close");
    }

    @Override
    public void invoke(RedisDataWithName value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == parameterTool.getInt("jedis.sync.size",5)){

            Map<String,List<RedisDataWithName>> dataWithName = new HashMap<>();

            for (RedisDataWithName bufferedElement : bufferedElements) {
                List<RedisDataWithName> data = dataWithName.getOrDefault(bufferedElement.getRedisName(),new ArrayList<>());
                data.add(bufferedElement);
                dataWithName.put(bufferedElement.getRedisName(),data);
            }
            bufferedElements.clear();

            for (Map.Entry<String, List<RedisDataWithName>> stringListEntry : dataWithName.entrySet()) {
                Jedis jedis = null;
                Pipeline pipeline = null;
                JedisPool pool = RedisHelperWithPools.getJedisPool(stringListEntry.getKey());
                if (pool != null) {
                    try {
                        jedis = pool.getResource();
                        pipeline = jedis.pipelined();
                        for (RedisDataWithName redisDataWithName : stringListEntry.getValue()) {
                            RedisHelperWithPools.saveRedisData(pipeline, redisDataWithName);
                        }
                    } catch (JedisConnectionException e) {
                        RedisHelperWithPools.restartPool(stringListEntry.getKey());
                        errorInc(stringListEntry.getKey());
                        logger.error("pools restart", e);
                    } catch (Exception e) {
                        errorInc(stringListEntry.getKey());
                        logger.error("invoke error", e);
                    } finally {
                        RedisHelperWithPools.returnResource(jedis, pipeline);
                    }
                }
            }
        }
        Map<String,Object> retains = new HashMap<>();
        retains.put("data",bufferedElements.size());
        dataRetainMonitor.setJsonMonitor(retains);
    }

    private void errorInc(String name){
        Integer batchSinkError = Integer.parseInt(errorMonitor.getJsonMonitor().getOrDefault(name,"0").toString())+1;
        errorMonitor.getJsonMonitor().put(name,batchSinkError);
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
            for (RedisDataWithName bufferedElement : bufferedElements) {
                checkpointedState.add(bufferedElement);
            }
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

            ListStateDescriptor<RedisDataWithName> descriptor = new ListStateDescriptor<RedisDataWithName>(
                    "buffered-elements",
                    TypeInformation.of(new TypeHint<RedisDataWithName>() {})
            );

            checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

            if (functionInitializationContext.isRestored()) {
                bufferedElements = new ArrayList<>();
                for (RedisDataWithName redisData : checkpointedState.get()) {
                    bufferedElements.add(redisData);
                }
            }
            logger.debug("initialize state completed");
        }catch (Exception e){
            logger.error("initialize state error",e);
        }
    }
}
