package com.lhs.flink.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.flink.metrics.Gauge;

import java.util.Map;

/**
 * @ClassNameGaugeMonitor
 * @Description
 * @Author lihuasong
 * @Date2020/5/30 9:39
 * @Version V1.0
 **/
public class GaugeMonitor implements Gauge<Map<String, Integer>> {

    private Map<String,Integer> logMonitor;

    public GaugeMonitor() {
    }

    public GaugeMonitor(Map<String, Integer> logMonitor) {
        this.logMonitor = logMonitor;
    }

    @Override
    public Map<String, Integer> getValue() {
        return this.getLogMonitor();
    }

    public Map<String, Integer> getLogMonitor() {
        return logMonitor;
    }

    public void setLogMonitor(Map<String, Integer> logMonitor) {
        this.logMonitor = logMonitor;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this.getLogMonitor());
    }
}
