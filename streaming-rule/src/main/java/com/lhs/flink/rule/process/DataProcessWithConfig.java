package com.lhs.flink.rule.process;

import com.lhs.flink.rule.dao.LogConfigMapper;
import com.lhs.flink.rule.dao.MybatisSessionFactory;
import com.lhs.flink.rule.engine.EngineManager;
import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisData;
import com.lhs.flink.rule.utils.LogConfigUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class DataProcessWithConfig extends BroadcastProcessFunction<String,Map<String,String>, RedisData>{
    private static final Logger logger = LoggerFactory.getLogger(DataProcessWithConfig.class);

    private ParameterTool parameterTool;
    /**
     * 每个日志配置的json串
     */
    private String logProcessConfigs;

    private EngineManager engineManager;

    public DataProcessWithConfig() {
    }

    public DataProcessWithConfig(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

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
            engineManager.initEngines(logProcessConfigs);

            logger.info("configs init");
        }catch (Exception e){
            logger.error("init error",e);
        }finally {
            MybatisSessionFactory.closeSession(sqlSession);
            logger.info("sql session closed");
        }

    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<RedisData> collector) throws Exception {
        for (RedisData redisData : engineManager.getRedisDatas(s)) {
            collector.collect(redisData);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<RedisData> collector) throws Exception {
        this.logProcessConfigs = stringMapMap.get("log_process_config");
        engineManager.reload(logProcessConfigs);
    }



}
