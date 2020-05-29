package com.lhs.flink.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.utils.LogConfigUtils;
import com.lhs.flink.utils.RecoveryData;
import com.mysql.jdbc.log.LogUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.everit.json.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/21
 **/
public class DataWashWithConfig extends BroadcastProcessFunction<String,Map<String,String>,String>{
    private static final Logger logger = LoggerFactory.getLogger(DataWashWithConfig.class);

    private String logConfigs;
    private Map<String,Schema> validateSchemas;
    private Map<String,Map<String,Map<String,String>>> recoverAttributes;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // @TODO 需要连接数据库，初始化配置
        validateSchemas = new HashMap<>();
        recoverAttributes = new HashMap<>();
    }

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

        int status = 0;

        JSONObject object = JSON.parseObject(s);
        String logType = object.getString("type");
        System.out.println(s);
        if (this.recoverAttributes.containsKey(logType)){
            try {
                RecoveryData.recoveryJsonByAttribute(object, this.recoverAttributes);
                logger.info("wash log data logtype = {}",logType);
                status = 3;
            }catch (Exception e){
                status = 4;
                logger.error("wash log data logtype = {}",logType,e);
            }
        }
//
        if(this.validateSchemas.containsKey(logType)){
            try {
                this.validateSchemas.get(logType).validate(new org.json.JSONObject(object.toString()));
                status = 1;
                logger.info("validate log data logtype = {}",logType);
            }catch (Exception e){
                status = 2;
                logger.error("validate log data error logtype = {}",logType,e);
            }
        }

        collector.collect(status +" : "+ object.toString());
    }

    @Override
    public void processBroadcastElement(Map<String, String> stringMapMap, Context context, Collector<String> collector) throws Exception {
        this.logConfigs = stringMapMap.get("log_config");
//        this.recoverAttributes = this.logConfig.getRecoverAttributes();
//        this.validateSchemas = this.logConfig.getValidateSchemas();
        this.validateSchemas = LogConfigUtils.initSchema(logConfigs);
        this.recoverAttributes = LogConfigUtils.initRecoverAttris(logConfigs);
    }
}
