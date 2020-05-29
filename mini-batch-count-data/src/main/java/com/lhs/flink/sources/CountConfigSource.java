package com.lhs.flink.sources;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/25
 **/
public class CountConfigSource extends RichSourceFunction<Map<String,Map<String,String>>>{

    private boolean running = true;

    public CountConfigSource(){};

    public CountConfigSource(Properties properties){

    }

    @Override
    public void run(SourceContext<Map<String, Map<String, String>>> sourceContext) throws Exception {
        while (running){
            // @TODO 从接口或者MySQL中获取日志的配置数据
            Map<String,Map<String,String>> map = new HashMap<>();

            Map<String,String> config = getTestConfig();

            map.put("count_config",config);

            sourceContext.collect(map);

            Thread.sleep(60000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private Map<String,String> getTestConfig(){

        Map<String,String> map = new HashMap<>();

        map.put("log_type","log_config");

        return map;
    }
}
