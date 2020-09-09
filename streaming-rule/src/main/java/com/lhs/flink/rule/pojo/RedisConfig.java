package com.lhs.flink.rule.pojo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/26
 **/
public class RedisConfig {
    private Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    private int id;
    private String redisName;
    private String properties;
    private int status;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRedisName() {
        return redisName;
    }

    public void setRedisName(String redisName) {
        this.redisName = redisName;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Map<String,String> getJsonConfig(){
        Map<String, String> config = new HashMap<>();
        try {
            config.put("redis_name", redisName);
            config.put("status", String.valueOf(status));
            for (String s : properties.split(",")) {
                String[] fs = s.split("=");
                config.put(fs[0].trim(),fs[1].trim());
            }
        }catch (Exception e){
            logger.error("redis config serialize error",e);
        }
        return config;
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "id=" + id +
                ", redisName='" + redisName + '\'' +
                ", properties='" + properties + '\'' +
                ", status=" + status +
                '}';
    }
}
