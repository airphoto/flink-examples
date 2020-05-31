package com.lhs.flink.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.GaugeMonitor;
import com.lhs.flink.pojo.LogConfig;
import com.lhs.flink.utils.LogConfigUtils;
import com.lhs.flink.utils.MonitorUtils;
import com.lhs.flink.utils.RecoveryData;
import com.lhs.flink.utils.RedisHelper;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.session.SqlSession;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class DataWashWithConfig extends BroadcastProcessFunction<String,Map<String,String>, Tuple2<String,String>>{
    private static final Logger logger = LoggerFactory.getLogger(DataWashWithConfig.class);

    private transient Map<String, Integer> logMonitor;

    /**
     * 每个日志配置的json串
     */
    private String logConfigs;

    /**
     * 每个日志的schema
     */
    private Map<String,Schema> validateSchemas;

    /**
     * 每个日志恢复数据的配置
     */
    private Map<String,Map<String,Map<String,String>>> recoverAttributes;

    /**
     * 每个日志对应的sink topic
     */
    private Map<String,String> topicMap;

    /**
     * redis 相关的配置
     */
    private Properties redisPropertis;

    public DataWashWithConfig() {
    }

    public DataWashWithConfig(Properties redisPropertis) {
        this.redisPropertis = redisPropertis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SqlSession sqlSession = null;
        try{
            // mysql 连接 初始化
            sqlSession = MybatisSessionFactory.getSession();
            RedisHelper.makePool(redisPropertis);

            LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
            List<LogConfig> logConfigs = mapper.queryLogConfig();
            this.logConfigs = LogConfigUtils.getSchemaJson(logConfigs);
            this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
            this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
            this.topicMap = LogConfigUtils.initSinkTopic(this.logConfigs);

            logMonitor = ExpiringMap.builder().expiration(1, TimeUnit.DAYS).build();
            getRuntimeContext().getMetricGroup().gauge("log_gauge",new GaugeMonitor(logMonitor));
            logger.info("configs init");
        }catch (Exception e){
            logger.error("init error",e);
        }finally {
            MybatisSessionFactory.closeSession(sqlSession);
            logger.info("sql session closed");
        }

    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Tuple2<String,String>> collector) throws Exception {
        try {
            JSONObject object = JSON.parseObject(s);
            String logType = object.getString("type");
            if (this.recoverAttributes != null && this.recoverAttributes.containsKey(logType)) {
                try {
                    RecoveryData.recoveryJsonByAttribute(object, this.recoverAttributes, logMonitor);
                    logger.info("wash log data log type = {}", logType);
                } catch (Exception e) {
                    logger.error("wash log data log type = {}", logType);
                }
            }

            if (this.validateSchemas != null && this.validateSchemas.containsKey(logType)) {
                try {
                    this.validateSchemas.get(logType).validate(new org.json.JSONObject(object.toString()));
                    MonitorUtils.normalInc(logType, this.logMonitor);
                    logger.info("validate log data log type = {}", logType);
                } catch (ValidationException e) {
                    MonitorUtils.errorInc(logType, this.logMonitor, e.getAllMessages());
                    logger.error("validate log data error log type = {},error messages = {}", logType, e.getAllMessages());
                }
            }
            Tuple2<String, String> washData = new Tuple2<>("wash", object.toString());
            logger.info("topic = " + washData.f0 + "; value = " + washData.f1);
            collector.collect(washData);
        }catch (Exception e){
            logger.error("error json logs [{}]",s);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<Tuple2<String,String>> collector) throws Exception {
        this.logConfigs = stringMapMap.get("log_config");
        this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
        this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
        this.topicMap = LogConfigUtils.initSinkTopic(this.logConfigs);


        System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
        System.out.println(getRuntimeContext().getAttemptNumber());
        System.out.println(getRuntimeContext().getTaskName());
        System.out.println(getRuntimeContext().getIndexOfThisSubtask());
        System.out.println(getRuntimeContext().getMaxNumberOfParallelSubtasks());


        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = RedisHelper.getJedis();
            pipeline = jedis.pipelined();
            RedisHelper.saveMetricData(pipeline,this.logMonitor,getRuntimeContext().getTaskName());
            logger.info("jedis db ["+jedis.getDB()+"] metric monitor ["+JSON.toJSONString(this.logMonitor)+"]");
        }catch (Exception e){
            logger.error("metric data save error",e);
        }finally {
            RedisHelper.returnSource(jedis,pipeline);
        }
    }



}
