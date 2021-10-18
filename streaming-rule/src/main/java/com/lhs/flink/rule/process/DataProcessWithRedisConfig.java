package com.lhs.flink.rule.process;

import com.alibaba.fastjson.JSON;
import com.lhs.flink.rule.dao.LogConfigMapper;
import com.lhs.flink.rule.dao.MybatisSessionFactory;
import com.lhs.flink.rule.engine.EngineManager;
import com.lhs.flink.rule.monitor.JsonGuigeMonitor;
import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisConfig;
import com.lhs.flink.rule.pojo.RedisDataWithName;
import com.lhs.flink.rule.sink.RedisHelperWithPools;
import com.lhs.flink.rule.utils.LogConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class DataProcessWithRedisConfig extends BroadcastProcessFunction<String,Map<String,String>, RedisDataWithName>{
    private static final Logger logger = LoggerFactory.getLogger(DataProcessWithRedisConfig.class);

    /**
     * 每个日志配置的json串
     */
    private String logProcessConfigs;

    /**
     * redis的配置表
     */
    private String redisConfigStr;

    private EngineManager engineManager;

    private transient JsonGuigeMonitor enginesMonitor;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SqlSession sqlSession = null;
        try{
            // mysql 连接 初始化
            sqlSession = MybatisSessionFactory.getSession();

            LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
            List<LogConfig> logConfigs = mapper.queryLogConfig();
            logProcessConfigs = LogConfigUtils.serializeConfigs(logConfigs);

            engineManager = EngineManager.getInstance();
            engineManager.reload(logProcessConfigs);

            List<RedisConfig> redisConfigList = mapper.queryRedisConfig();
            this.redisConfigStr = LogConfigUtils.serializeRedisConfigs(redisConfigList);
            RedisHelperWithPools.init(this.redisConfigStr);
            Map<String,Object> monitorMap = new HashMap<>();

            Map<String, Object> existPools = RedisHelperWithPools.getExistPools();
            Map<String, Object> existEngine = engineManager.getExistEngine();

            monitorMap.putAll(existPools);
            monitorMap.putAll(existEngine);

            enginesMonitor = getRuntimeContext().getMetricGroup().gauge("gauge_engines",new JsonGuigeMonitor(monitorMap));

            logger.info("configs init");
        }catch (Exception e){
            logger.error("init error",e);
        }finally {
            MybatisSessionFactory.closeSession(sqlSession);
            logger.info("sql session closed");
        }

    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<RedisDataWithName> collector) throws Exception {
        for (RedisDataWithName redisData : engineManager.getRedisDatasWithName(s)) {
            collector.collect(redisData);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<RedisDataWithName> collector) throws Exception {
        try {
            this.logProcessConfigs = stringMapMap.get("log_process_config");
            this.redisConfigStr = stringMapMap.get("redis_config");
            engineManager.reload(logProcessConfigs);
            RedisHelperWithPools.init(this.redisConfigStr);
            enginesMonitor.getJsonMonitor().putAll(RedisHelperWithPools.getExistPools());
            enginesMonitor.getJsonMonitor().putAll(engineManager.getExistEngine());
            logger.info("redis pool exists [{}], engine exists [{}]", JSON.toJSONString(RedisHelperWithPools.getExistPools()),JSON.toJSONString(engineManager.getExistEngine()));
        }catch (Exception e){
            logger.error("processBroadcastElement error",e);
        }
    }



}
