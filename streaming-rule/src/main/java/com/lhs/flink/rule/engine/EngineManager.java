package com.lhs.flink.rule.engine;

import com.lhs.flink.rule.pojo.LogConfig;
import com.lhs.flink.rule.pojo.RedisData;
import com.lhs.flink.rule.utils.LogConfigUtils;

import java.util.*;
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
    private final Map<Integer,MySQLLoadJavaScriptEngine> engines = new HashMap<>();
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

    public void initEngines(String engineString){
        logConfigs = LogConfigUtils.deserializeConfigs(engineString);

        for (LogConfig logConfig : logConfigs) {
            if(!engines.keySet().contains(logConfig.getId())) {
                MySQLLoadJavaScriptEngine mySQLLoadJavaScriptEngine = new MySQLLoadJavaScriptEngine(logConfig.getProcessJS());
                mySQLLoadJavaScriptEngine.initEngine();
                engines.put(logConfig.getId(), mySQLLoadJavaScriptEngine);
            }
        }


    }

    public void reload(String engineString){
        initEngines(engineString);

        Set<Integer> configIds = logConfigs.stream().map(LogConfig::getId).collect(Collectors.toSet());
        for (Integer engineId : engines.keySet()) {
            if(!configIds.contains(engineId)){
                engines.remove(engineId);
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

    public List<RedisData> getRedisDatas(String data){
        List<RedisData> result = new ArrayList<>();
        for (Map.Entry<Integer, MySQLLoadJavaScriptEngine> engineEntry : engines.entrySet()) {
            RedisData redisData = engineEntry.getValue().getRedisData(data);
            if (redisData != null) {
                result.add(redisData);
            }
        }
        return result;
    }

}
