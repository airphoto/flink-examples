package com.lhs.flink.rule.monitor;

import com.alibaba.fastjson.JSON;
import org.apache.flink.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 文件名称：JsonGuigeMonitor
 * 创建时间：2020-08-03
 * 描述：
 *
 * @author lihuasong
 * @version v1.0
 * 更新 ：[0] 2020-08-03 lihuasong [变更内容]
 **/
public class JsonGuigeMonitor implements Gauge<String>{

    private static final Logger logger = LoggerFactory.getLogger(JsonGuigeMonitor.class);

    private Map<String,Object> jsonMonitor;

    public JsonGuigeMonitor(Map<String,Object> jsonMonitor){
        this.jsonMonitor = jsonMonitor;
    }

    @Override
    public String getValue() {
        String result = null;
        try {
            result = JSON.toJSONString(this.getJsonMonitor());
        }catch (Exception e){
            logger.error("gauge monitor data error",e);
        }
        return result;
    }

    public Map<String, Object> getJsonMonitor() {
        return jsonMonitor;
    }

    public void setJsonMonitor(Map<String, Object> jsonMonitor) {
        this.jsonMonitor = jsonMonitor;
    }
}
