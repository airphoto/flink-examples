package com.lhs.flink.rule.engine;

import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisDataWithName;
import com.lhs.flink.rule.utils.LogConfigUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 文件名称：EngineManager
 * 创建时间：2020-07-03
 * 描述：
 *          管理VM
 * @author lihuasong
 * @version v1.0
 * 更新 ：[0] 2020-07-03 lihuasong [变更内容]
 **/
public class EngineManager{
    volatile private static EngineManager instance = null;
    private final Map<Integer,MySQLLoadJavaScriptEngine> engines = new ConcurrentHashMap<>();
    private List<LogConfig> logConfigs = null;
    private EngineManager(){}

    public static EngineManager getInstance(){
        if(instance==null){
            synchronized (EngineManager.class){
                if(instance == null){
                    instance = new EngineManager();
                }
            }
        }

        return instance;
    }

    public void reload(String engineString){
        synchronized (engines) {
            logConfigs = LogConfigUtils.deserializeConfigs(engineString);
            for (LogConfig logConfig : logConfigs) {
                if (!engines.keySet().contains(logConfig.getId())) {
                    MySQLLoadJavaScriptEngine mySQLLoadJavaScriptEngine = new MySQLLoadJavaScriptEngine(logConfig.getProcessJS());
                    mySQLLoadJavaScriptEngine.initEngine();
                    engines.put(logConfig.getId(), mySQLLoadJavaScriptEngine);
                }
            }
            Set<Integer> configIds = logConfigs.stream().map(LogConfig::getId).collect(Collectors.toSet());
            for (Integer engineId : engines.keySet()) {
                if (!configIds.contains(engineId)) {
                    engines.remove(engineId);
                }
            }
        }
    }

    public List<Optional<String>> executeAll(String data){
        List<Optional<String>> result = new ArrayList<>();
        for (Map.Entry<Integer, MySQLLoadJavaScriptEngine> engineEntry : engines.entrySet()) {
            result.add(engineEntry.getValue().processData(data));
        }
        return result;
    }

    public List<RedisDataWithName> getRedisDatasWithName(String data){
        List<RedisDataWithName> result = new ArrayList<>();
        for (Map.Entry<Integer, MySQLLoadJavaScriptEngine> engineEntry : engines.entrySet()) {
            List<RedisDataWithName> redisData = engineEntry.getValue().getRedisDataWithName(data);
            if(redisData != null) {
                for (RedisDataWithName redisDatum : redisData) {
                    if (redisDatum != null) {
                        result.add(redisDatum);
                    }
                }
            }
        }
        return result;
    }

    public Map<String,Object> getExistEngine(){
        Map<String,Object> result = new HashMap<>();
        List<Map<String,Object>> data = new ArrayList<>();

        engines.forEach((id,engine)->{
            Map<String,Object> existsEngines = new HashMap<>();
            existsEngines.put("id",id);
            existsEngines.put("alive",engine!=null);
            data.add(existsEngines);
        });
        result.put("engines",data);
        return result;
    }

}
