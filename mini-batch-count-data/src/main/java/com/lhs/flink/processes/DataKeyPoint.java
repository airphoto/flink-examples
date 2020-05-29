package com.lhs.flink.processes;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/25
 **/
public class DataKeyPoint extends BroadcastProcessFunction<String,Map<String,Map<String,String>>, Tuple2<String,String>> {

    private Map<String,String> countCounfig;

    @Override
    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
        // @TODO 根据配置文件，配置key，和统计数据的各种方法
        String key = "";
        String value = "";
        Tuple2 data = new Tuple2(key,value);

        collector.collect(data);
    }

    @Override
    public void processBroadcastElement(Map<String, Map<String, String>> stringMapMap, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        this.countCounfig = stringMapMap.get("count_config");
    }
}
