package com.lhs.flink.rule.engine;

import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.lhs.flink.rule.utils.LogConfigUtils;
import redis.clients.jedis.Jedis;

/**
 * @ClassNameEngineTest
 * @Description
 * @Author lihuasong
 * @Date2020/7/4 8:28
 * @Version V1.0
 **/
public class EngineTest {

    private static EngineManager engineManager = EngineManager.getInstance();

    private static List<LogConfig> configs = new ArrayList<>();


        static {
            LogConfig config = new LogConfig();
            config.setId(1);
            config.setFunctionName("process");
            config.setStatus(1);
            config.setProcessJS("function process_data(a) {return '{\"db\":1,\"redisType\":1,\"key\":\"key\",\"field\":\"field\",\"value\":\"a\",\"time\":\"time\",\"ttl\":1000}'}");

            configs.add(config);
        }

    @Test
    public void testEngineInit(){


        String data = LogConfigUtils.serializeConfigs(configs);

         engineManager.initEngines(data);
        List<Optional<String>> test = engineManager.executeAll("test");
        for (Optional<String> s : test) {
            System.out.println(s.get());
        }
    }

    @Test
    public void testRedisData(){
            String data = LogConfigUtils.serializeConfigs(configs);
            engineManager.initEngines(data);
            List<RedisData> test = engineManager.getRedisDatas("test_redis");
        for (RedisData redisData : test) {
            System.out.println(redisData);
        }
    }

    @Test
    public void testValue(){
        Jedis jedis = new Jedis("10.122.238.97",16379);
        jedis.auth("XLhy!321YH");
        jedis.select(1);
        System.out.println(jedis.get("10001:88888"));

    }
}
