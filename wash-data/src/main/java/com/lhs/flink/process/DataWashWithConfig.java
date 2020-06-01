package com.lhs.flink.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.GaugeMonitor;
import com.lhs.flink.pojo.LogConfig;
import com.lhs.flink.utils.DateUtils;
import com.lhs.flink.utils.LogConfigUtils;
import com.lhs.flink.utils.MonitorUtils;
import com.lhs.flink.utils.RecoveryData;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.session.SqlSession;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class DataWashWithConfig extends BroadcastProcessFunction<String,Map<String,String>, Tuple2<String,String>>{
    private static final Logger logger = LoggerFactory.getLogger(DataWashWithConfig.class);

    private transient GaugeMonitor gaugeMonitor;

    private ParameterTool parameterTool;
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


    public DataWashWithConfig() {
    }

    public DataWashWithConfig(ParameterTool parameterTool) {
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
            this.logConfigs = LogConfigUtils.getSchemaJson(logConfigs);
            this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
            this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
            this.topicMap = LogConfigUtils.initSinkTopic(this.logConfigs);
            Map<String,Integer> metricMap = new HashMap<>();
            ExpiringMap<String,Map<String,Integer>> monitorMap = ExpiringMap
                    .builder()
                    .expiration(this.parameterTool.getLong("metric.map.ttl",30),TimeUnit.SECONDS)
                    .expirationPolicy(ExpirationPolicy.ACCESSED)
                    .build();
            monitorMap.put(DateUtils.getTimeByFormat(System.currentTimeMillis(),"yyyyMMdd"),metricMap);
            System.out.println("monitor map expiration : "+monitorMap.getExpiration());
            this.gaugeMonitor = getRuntimeContext().getMetricGroup().gauge("log_gauge",new GaugeMonitor(monitorMap));

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
                    RecoveryData.recoveryJsonByAttribute(object, this.recoverAttributes, gaugeMonitor);
                    logger.info("wash log data log type = {}", logType);
                } catch (Exception e) {
                    logger.error("wash log data log type = {}", logType);
                }
            }

            if (this.validateSchemas != null && this.validateSchemas.containsKey(logType)) {
                try {
                    this.validateSchemas.get(logType).validate(new org.json.JSONObject(object.toString()));
                    MonitorUtils.normalInc(logType, this.gaugeMonitor);
                    logger.info("validate log data log type = {}", logType);
                } catch (ValidationException e) {
                    MonitorUtils.errorInc(logType, this.gaugeMonitor, e.getAllMessages());
                    logger.error("validate log data error log type = {},error messages = {}", logType, e.getAllMessages());
                }
            }
            Tuple2<String, String> washData = new Tuple2<>(this.topicMap.getOrDefault(logType,this.parameterTool.get("kafka.sink.default.topic","wash")), object.toString());
            MonitorUtils.monitorInc("topic:"+washData.f0,this.gaugeMonitor);

            collector.collect(washData);
            MonitorUtils.monitorInc("all",this.gaugeMonitor);
        }catch (Exception e){
            Tuple2<String, String> errorJsonData = new Tuple2<>(this.parameterTool.get("kafka.sink.error.topic","error"), s);
            collector.collect(errorJsonData);
            MonitorUtils.monitorInc("topic:"+errorJsonData.f0,this.gaugeMonitor);
            MonitorUtils.monitorInc("all",this.gaugeMonitor);
            logger.error("error json logs [{}]",s);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<Tuple2<String,String>> collector) throws Exception {
        this.logConfigs = stringMapMap.get("log_config");
        this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
        this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
        this.topicMap = LogConfigUtils.initSinkTopic(this.logConfigs);
    }



}
