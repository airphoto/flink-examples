package com.lhs.flink.rule.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.rule.pojo.LogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/29
 **/
public class LogConfigUtils {

    private static final Logger logger = LoggerFactory.getLogger(LogConfigUtils.class);

    /**
     *  将Configs转化成json数据
     * @param logConfigs
     * @return
     */
    public static String serializeConfigs(List<LogConfig> logConfigs){
        String logConfigJson = null;
        try{
            logConfigJson = JSONObject.toJSONString(logConfigs);
        }catch (Exception e){
            logger.error("serialize configs error",e);
        }
        return logConfigJson;
    }

    public static List<LogConfig> deserializeConfigs(String logConfigJson){
        List<LogConfig> configs = null;
        try {
            configs = JSONArray.parseArray(logConfigJson, LogConfig.class);
        }catch (Exception e){
            logger.error("deserialize configs error");
        }
        return configs;
    }

    public static void main(String[] args) {
        LogConfig config = new LogConfig();
        config.setId(1);
        config.setFunctionName("process");
        config.setStatus(1);
        config.setProcessJS("process");

        List<LogConfig> configs = new ArrayList<>();
        configs.add(config);

        String data = serializeConfigs(configs);

        System.out.println(deserializeConfigs(data));
    }
}
