package com.lhs.flink.sources;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.LogConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.ibatis.session.SqlSession;
import org.everit.json.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lihuasong
 * @description
 *      定时获取schema和filter数据
 * @create 2020/5/21
 **/
public class LogConfigSource extends RichSourceFunction<Map<String,String>> {

    private static final Logger logger = LoggerFactory.getLogger(LogConfigSource.class);

    private boolean running = true;
    private long sleepMs;

    public LogConfigSource(long sleepMs){
        this.sleepMs = sleepMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void run(SourceContext<Map<String,String>> sourceContext) {
        while (running){
            SqlSession sqlSession= null;
            try {
                sqlSession = MybatisSessionFactory.getSession();
                LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
                List<LogConfig> logConfigs = mapper.queryLogConfig();
                Map<String, String> map = new HashMap<>();

                map.put("log_config", getSchemaJson(logConfigs));

                sourceContext.collect(map);

                Thread.sleep(this.sleepMs);

                logger.info("get log_config succes",running);
            }catch (Exception e){
                logger.error("get log_config error",e);
            } finally {
                MybatisSessionFactory.closeSession(sqlSession);
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        logger.info("running is false and sqlSession is closed");
    }

    private String getSchemaJson(List<LogConfig> logConfigs){
        System.out.println(JSONObject.toJSONString(logConfigs));
        return JSONObject.toJSONString(logConfigs);
    }

//    private void setBroadConfigs(List<LogConfig> logConfigs,BroadConfig broadConfigs){
//        Map<String,Schema> logSchema = new HashMap<>();
//        Map<String,Map<String,Map<String,String>>> recoverAttributes = new HashMap<>();
//        logConfigs.forEach(logConfig -> {
//            Schema schema = logConfig.getLogSchemaValidator();
//            if (schema!=null){
//                logSchema.put(logConfig.getLogType(),schema);
//                Map<String, Map<String, String>> logRecoverAttribute = logConfig.getLogRecoverAttribute();
//                if(logRecoverAttribute!=null){
//                    recoverAttributes.put(logConfig.getLogType(),logRecoverAttribute);
//                }
//            }
//        });
//
//        broadConfigs.setRecoverAttributes(recoverAttributes);
//        broadConfigs.setValidateSchemas(logSchema);
//    }
}
