package com.lhs.flink.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @ClassNameGaugeMonitor
 * @Description
 * @Author lihuasong
 * @Date2020/5/30 9:39
 * @Version V1.0
 **/
public class GaugeMonitor implements Gauge<String> {

    private static final Logger logger = LoggerFactory.getLogger(GaugeMonitor.class);

    private Map<String,Map<String,Integer>> logMonitor;

    public GaugeMonitor() {
    }

    public GaugeMonitor(Map<String,Map<String,Integer>> logMonitor){
        this.logMonitor = logMonitor;
    }

    @Override
    public String getValue() {
        String result = null;
        try{
            result = JSON.toJSONString(this.getLogMonitor());
        }catch (Exception e){
            logger.error("gauge data error",e);
        }
        return result;
    }

    public Map<String, Map<String, Integer>> getLogMonitor() {
        return logMonitor;
    }

    public void setLogMonitor(Map<String, Map<String, Integer>> logMonitor) {
        this.logMonitor = logMonitor;
    }
}
