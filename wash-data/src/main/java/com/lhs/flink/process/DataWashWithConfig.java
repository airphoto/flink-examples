package com.lhs.flink.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.dao.LogConfigMapper;
import com.lhs.flink.dao.MybatisSessionFactory;
import com.lhs.flink.pojo.GaugeMonitor;
import com.lhs.flink.pojo.LogConfig;
import com.lhs.flink.utils.LogConfigUtils;
import com.lhs.flink.utils.RecoveryData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.session.SqlSession;
import org.everit.json.schema.Schema;
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
public class DataWashWithConfig extends BroadcastProcessFunction<String,Map<String,String>,String>{
    private static final Logger logger = LoggerFactory.getLogger(DataWashWithConfig.class);

    private transient Map<String, Integer> logMonitor;

    private String logConfigs;
    private Map<String,Schema> validateSchemas;
    private Map<String,Map<String,Map<String,String>>> recoverAttributes;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // @TODO 需要连接数据库，初始化配置
        SqlSession sqlSession = null;
        try{
            sqlSession = MybatisSessionFactory.getSession();
            LogConfigMapper mapper = sqlSession.getMapper(LogConfigMapper.class);
            List<LogConfig> logConfigs = mapper.queryLogConfig();
            this.logConfigs = LogConfigUtils.getSchemaJson(logConfigs);
            this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
            this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
            logMonitor = new HashMap<>();
            getRuntimeContext().getMetricGroup().gauge("log_guage",new GaugeMonitor(logMonitor));
            logger.info("configs inited");
        }catch (Exception e){
            logger.error("init error",e);
        }finally {
            MybatisSessionFactory.closeSession(sqlSession);
            logger.info("sql session closed");
        }

    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
        // @TODO 统计每种类型的recover的数据
        int status = 0;
        JSONObject object = JSON.parseObject(s);
        String logType = object.getString("type");
        System.out.println(s);
        if (this.recoverAttributes.containsKey(logType)){
            try {
                RecoveryData.recoveryJsonByAttribute(object, this.recoverAttributes);
                status = 3;
                logger.info("wash log data logtype = {}",logType);
            }catch (Exception e){
                status = 4;
                logger.error("wash log data logtype = {}",logType);
            }
        }

        if(this.validateSchemas.containsKey(logType)){
            try {
                this.validateSchemas.get(logType).validate(new org.json.JSONObject(object.toString()));
                status = 1;
                logger.info("validate log data logtype = {}",logType);
            }catch (Exception e){
                status = 2;
                logger.error("validate log data error logtype = {}",logType);
            }
        }
        this.logMonitor.put(logType,this.logMonitor.getOrDefault(logType,0)+1);
        collector.collect(status +" : "+ object.toString());
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<String> collector) throws Exception {
        this.logConfigs = stringMapMap.get("log_config");
        this.validateSchemas = LogConfigUtils.initSchema(this.logConfigs);
        this.recoverAttributes = LogConfigUtils.initRecoverAttris(this.logConfigs);
        // @TODO 将统计数据存储到redis
        this.logMonitor.forEach((k,v)-> System.out.println(k+"->"+v));
    }
}
