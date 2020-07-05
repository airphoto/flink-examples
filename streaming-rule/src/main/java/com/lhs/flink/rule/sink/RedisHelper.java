package com.lhs.flink.rule.sink;

import com.lhs.flink.rule.pojo.RedisData;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import com.lhs.flink.rule.utils.ImplicitUtils.StringUtils;
/**
 * @ClassNameRedisHelper
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 10:29
 * @Version V1.0
 **/
public class RedisHelper {

    public static final int STRING_PUT = 0;
    public static final int STRING_INCREASE = 1;
    public static final int HASH_PUT = 2;
    public static final int HASH_INCREASE = 3;
    public static final int SET_PUSH = 4;
    public static final int SET_REMOVE = 5;
    public static final int KEY_REMOVE = 6;


    private static Logger logger = LoggerFactory.getLogger(RedisHelper.class);

    private transient static JedisPool jedisPool;

    public static void mkPool(ParameterTool parameterTool){
        if(jedisPool == null){
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            config.setMaxTotal(parameterTool.getInt("jedis.max.total",5));
            config.setMaxIdle(parameterTool.getInt("jedis.max.idle",10));
            config.setMinIdle(parameterTool.getInt("jedis.min.idle",5));
            config.setTestOnBorrow(parameterTool.getBoolean("jedis.test.borrow",false));
            config.setTestOnReturn(parameterTool.getBoolean("jedis.test.borrow",false));
            config.setMaxWaitMillis(parameterTool.getLong("jedis.max.wait.ms",60000));
            jedisPool = new JedisPool(config,parameterTool.get("jedis.hosts","10.122.238.97"),
                    parameterTool.getInt("jedis.port",16379),
                    parameterTool.getInt("jedis.timeout",5000),
                    parameterTool.get("jedis.password","XLhy!321YH"),
                    parameterTool.getInt("jedis.db",1)
                    );
        }
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }

    public static void returnSource(Jedis jedis, Pipeline pipeline){
        try{
            if(pipeline!=null){
                pipeline.sync();
                pipeline.close();
            }
        }catch (Exception e){
            logger.error("jedis pipeline close error",e);
        }finally {
            if (jedis != null){
                try {
                    jedis.close();
                }catch (Exception e){
                    logger.error("jedis close error",e);
                }
            }
        }
    }

    public static void saveRedisData(Pipeline pipeline, RedisData redisData){
        try {
            pipeline.select(redisData.getDb());
            switch (redisData.getRedisType()){
                case STRING_PUT : pipeline.set(redisData.getKey(),redisData.getValue());
                    break;
                case STRING_INCREASE : pipeline.incrBy(redisData.getKey(),new StringUtils(redisData.getValue()).str2Int());
                    break;
                case HASH_PUT : pipeline.hset(redisData.getKey(),redisData.getField(),redisData.getValue());
                    break;
                case HASH_INCREASE : pipeline.hincrBy(redisData.getKey(),redisData.getField(),new StringUtils(redisData.getValue()).str2Int());
                    break;
                case SET_PUSH : pipeline.sadd(redisData.getKey(),redisData.getValue());
                    break;
                case SET_REMOVE : pipeline.srem(redisData.getKey(),redisData.getValue());
                    break;
                case KEY_REMOVE : pipeline.del(redisData.getKey());
                    break;
                default:
                    logger.warn("error redis type : {}",redisData.getRedisType());
            }

            if(redisData.getTtl() > 0){
                pipeline.expire(redisData.getKey(),redisData.getTtl());
            }
        }catch (Exception e){
            logger.error("redis save error ",e);
        }
    }

    public static void destroy(){
        if(jedisPool != null){
            jedisPool.close();
        }
    }
}
