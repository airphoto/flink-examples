package com.lhs.flink.pojo;

import org.apache.commons.lang3.StringUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/26
 **/
public class LogConfig {
    private String logType;
    private String logSchema;
    private String columnRecover;
    private String sinkTopic;

    public String getSinkTopic() {
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public String getColumnRecover() {
        return columnRecover;
    }

    public void setColumnRecover(String columnRecover) {
        this.columnRecover = columnRecover;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogSchema() {
        return logSchema;
    }

    public void setLogSchema(String logSchema) {
        this.logSchema = logSchema;
    }
}
