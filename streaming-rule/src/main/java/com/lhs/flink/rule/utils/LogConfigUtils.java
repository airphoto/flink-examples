package com.lhs.flink.rule.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fastjson.JSON.parseArray;

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
            configs = parseArray(logConfigJson, LogConfig.class);
        }catch (Exception e){
            logger.error("deserialize configs error");
        }
        return configs;
    }

    public static String serializeRedisConfigs(List<RedisConfig> configs){
        String redisConfigJson = null;
        try{
            redisConfigJson = JSONObject.toJSONString(configs.stream().map(RedisConfig::getJsonConfig).toArray());
        }catch (Exception e){
            logger.error("serialize redis configs error",e);
        }
        return redisConfigJson;
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

        RedisConfig redisConfig = new RedisConfig();
        redisConfig.setId(1);
        redisConfig.setProperties("name=test\nhost=host");
        redisConfig.setRedisName("ceshi");
        redisConfig.setStatus(1);

        RedisConfig redisConfig2 = new RedisConfig();
        redisConfig2.setId(1);
        redisConfig2.setProperties("name=test2\nhost2=host");
        redisConfig2.setRedisName("ceshi2");
        redisConfig2.setStatus(1);


        List<RedisConfig> redisConfigList = new ArrayList<>();
        redisConfigList.add(redisConfig);
        redisConfigList.add(redisConfig2);

        String str = serializeRedisConfigs(redisConfigList);
        JSONArray objects = parseArray(str);
        objects.forEach(o-> System.out.println(((JSONObject)o).getString("name")));
    }
}
