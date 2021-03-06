package com.lhs.flink.rule.dao;


import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisConfig;

import java.util.List;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/26
 **/
public interface LogConfigMapper {
    List<LogConfig> queryLogConfig();
    List<RedisConfig> queryRedisConfig();
}
