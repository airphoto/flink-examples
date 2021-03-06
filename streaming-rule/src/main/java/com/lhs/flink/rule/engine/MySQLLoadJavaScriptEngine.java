package com.lhs.flink.rule.engine;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.lhs.flink.rule.pojo.RedisDataWithName;
import com.lhs.flink.rule.utils.LogConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.List;
import java.util.Optional;

/**
 * 文件名称：MySQLLoadJavaScriptEngine
 * 创建时间：2020-07-03
 * 描述：
 *    javascript引擎，处理json日志，返回redis需要的结果
 * @author lihuasong
 * @version v1.0
 * 更新 ：[0] 2020-07-03 lihuasong [变更内容]
 **/
public class MySQLLoadJavaScriptEngine {

    private final static String ENGINE_NOT_LOADED="Engine has not initialized";

    private Logger logger = LoggerFactory.getLogger(MySQLLoadJavaScriptEngine.class);

    private String initScript;
    private ScriptEngine engine;
    private Invocable invocable;

    public MySQLLoadJavaScriptEngine(String initScript){
        this.initScript = initScript;
    }

    /**
     * 初始化引擎
     */
    public void initEngine() {
        try{
            logger.info("loading engine for script {}",initScript);
            engine = new ScriptEngineManager().getEngineByName("nashorn");
            engine.eval(initScript);
            invocable = (Invocable)engine;
        }catch (Exception e){
            logger.error("loading engine for script error",e);
        }
    }

    /**
     * 处理数据，获得结果
     * @param data
     * @return
     */
    public Optional<String> processData(String data){
        try{
            if(engine == null){
                throw new RuntimeException(ENGINE_NOT_LOADED);
            }

            final Object result = invocable.invokeFunction("process_data",data);
            if(result != null){
                if(result instanceof String){
                    return Optional.of((String) result);
                }else{
                    logger.warn("function return a {} instead of java.lang.String",result.getClass().toString());
                    return Optional.of(result.toString());
                }
            }else{
                return Optional.empty();
            }
        }catch (Exception e){
            logger.error("process data error",e);
            return Optional.empty();
        }
    }

    /**
     * 处理数据，获得结果
     * @param data
     * @return
     */
    public List<RedisDataWithName> getRedisDataWithName(String data){
        Object result = null;
        try{
            if(engine == null){
                throw new RuntimeException(ENGINE_NOT_LOADED);
            }

            result = invocable.invokeFunction("process_data",data);
            logger.info("current thread {} ,engine process data {}",Thread.currentThread().getId(),result);
            if(result != null){
                return JSONArray.parseArray((String)result, RedisDataWithName.class);
            }else{
                return null;
            }
        }catch (Exception e){
            return null;
        }
    }


}
