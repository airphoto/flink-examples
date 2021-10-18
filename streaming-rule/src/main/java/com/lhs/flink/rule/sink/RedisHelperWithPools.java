package com.lhs.flink.rule.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.rule.pojo.RedisDataWithName;
import com.lhs.flink.rule.utils.ImplicitUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassNameRedisHelper
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 10:29
 * @Version V1.0
 **/
public class RedisHelperWithPools {

    public static final int STRING_PUT = 0;
    public static final int STRING_INCREASE = 1;
    public static final int HASH_PUT = 2;
    public static final int HASH_INCREASE = 3;
    public static final int SET_PUSH = 4;
    public static final int SET_REMOVE = 5;
    public static final int KEY_REMOVE = 6;


    private static Logger logger = LoggerFactory.getLogger(RedisHelperWithPools.class);

    private transient static ConcurrentHashMap<String,JedisPool> pools = new ConcurrentHashMap<>();
    private static String jedisConfigPoperties;
    public static void init(String propStrings){
        jedisConfigPoperties = propStrings;
        try{
            JSONArray objects = JSONObject.parseArray(propStrings);
            for (Object object : objects) {
                JSONObject obj = (JSONObject)object;
                String redisName = obj.getString("redis_name");
                int status = obj.getInteger("status");
                if (!pools.containsKey(redisName) && status==1) {
                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                    config.setMaxTotal(Integer.parseInt(obj.getOrDefault("jedis.max.total", "500").toString()));
                    config.setMaxIdle(Integer.parseInt(obj.getOrDefault("jedis.max.idle", "500").toString()));
                    config.setTestOnBorrow(Boolean.parseBoolean(obj.getOrDefault("jedis.test.borrow", "true").toString()));
                    config.setTestOnReturn(Boolean.parseBoolean(obj.getOrDefault("jedis.test.return", "true").toString()));
                    config.setTestWhileIdle(Boolean.parseBoolean(obj.getOrDefault("jedis.test.idle", "true").toString()));
                    config.setMaxWaitMillis(Long.parseLong(obj.getOrDefault("jedis.max.wait.ms", "10000").toString()));
                    String host = obj.getString("jedis.host");
                    String port = obj.getString("jedis.port");
                    String password = obj.getString("jedis.password");
                    String timeOut = obj.getOrDefault("jedis.timeout","5000").toString();
                    int db = 1;

                    JedisPool jedisPool = new JedisPool(config,host,Integer.parseInt(port),Integer.parseInt(timeOut),password,db);
                    pools.put(redisName,jedisPool);
                }
                if(pools.containsKey(redisName) && status != 1){
                    try {
                        pools.get(redisName).destroy();
                        pools.remove(redisName);
                    }catch (Exception e){
                        logger.error("destroy pool error",e);
                    }
                }
            }
        }catch (Exception e){
            logger.error("redis pool init error",e);
        }
    }

    public static JedisPool getJedisPool(String redisName) {
        return pools.getOrDefault(redisName,null);
    }

    public static void saveRedisData(Pipeline pipeline, RedisDataWithName redisData){
        try {
            pipeline.select(redisData.getDb());
            switch (redisData.getRedisType()){
                case STRING_PUT : pipeline.set(redisData.getKey(),redisData.getValue());
                    break;
                case STRING_INCREASE : pipeline.incrBy(redisData.getKey(),new ImplicitUtils.StringUtils(redisData.getValue()).str2Int());
                    break;
                case HASH_PUT : pipeline.hset(redisData.getKey(),redisData.getField(),redisData.getValue());
                    break;
                case HASH_INCREASE : pipeline.hincrBy(redisData.getKey(),redisData.getField(),new ImplicitUtils.StringUtils(redisData.getValue()).str2Int());
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

    public static void returnResources(Map<Jedis,Set<Pipeline>> jedisPipelineMap){
        jedisPipelineMap.forEach(((jedis, pipelines) -> {
            try{
                if(pipelines != null && !pipelines.isEmpty()) {
                    pipelines.forEach(pipeline -> {
                        try {
                            if (pipeline != null) {
                                pipeline.sync();
                                pipeline.close();
                            }
                        }
                        catch (Exception e){
                            logger.error("pipeline close error",e);
                        }
                    });
                }if(jedis!=null){
                    jedis.close();
                }
            }catch (Exception e){
                logger.error("jedis close error",e);
            }
        }));
    }

    public static void returnResource(Jedis jedis,Pipeline pipeline){
        try{
            pipeline.sync();
            pipeline.close();
        }catch (Exception e){
            logger.error("pipeline close error",e);
        }finally {
            try{
                jedis.close();
            }catch (Exception e2){
                logger.error("jedis close error",e2);
            }
        }
    }

    public static Map<String,Object> getExistPools() {
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> data = new ArrayList<>();
        pools.forEach((name,pool)->{
            Map<String,Object> existPools = new HashMap<>();
            existPools.put("pool_name",name);
            existPools.put("alive",false);
            try{
                pool.getResource().close();
                existPools.put("alive",true);
            }catch (Exception e){
                logger.error("redis pool error",e);
            }
            data.add(existPools);
        });
        result.put("pools",data);

        return result;
    }

    public static void restartPool(String name){
        try{
            pools.get(name).destroy();
            pools.remove(name);
        }catch (Exception e){
            logger.error("pool [{}] destroy error ",e,name);
        }

        init(jedisConfigPoperties);
    }

}
