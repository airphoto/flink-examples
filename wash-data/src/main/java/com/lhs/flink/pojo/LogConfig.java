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
public class LogConfig implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(LogConfig.class);

    private String logType;
    private String logSchema;
    private String columnRecover;

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

    @Override
    public String toString() {
        return "LogConfig{" +
                "logType='" + logType + '\'' +
                ", logSchema='" + logSchema + '\'' +
                '}';
    }

    public Schema getLogSchemaValidator(){
        Schema schema = null;
        try {
            schema = SchemaLoader.load(new JSONObject(new JSONTokener(this.getLogSchema())));
            logger.info("{} log schema load ",this.getLogType());
            return schema;
        }catch (Exception e){
            logger.error("{} log schema load error",this.getLogType());
        }
        return schema;
    }

    public Map<String,Map<String,String>> getLogRecoverAttribute(){

        Map<String,Map<String,String>> functionKey = null;

//        Map<String,String> columnNameRecover = this.getInnerMaps(this.getColumnRecover());
//
//        if(columnNameRecover !=null || columnTypeRecover != null){
//            functionKey = new HashMap<>();
//            if(columnNameRecover != null) {
//                functionKey.put("change_column_name", columnNameRecover);
//            }
//            if(columnTypeRecover != null) {
//                functionKey.put("change_column_type", columnTypeRecover);
//            }
//        }

        return functionKey;
    }

//    private Map<String,Map<String,String>> getInnerMaps(String innerValue){
//        if(StringUtils.isNoneBlank(innerValue) && !innerValue.equals("-")) {
//            Map<String,Map<String,String>> functionsKey = new HashMap<>();
//            try {
//                Map<String, String> innerKV = new HashMap<>();
//                JSONObject jsonObject = new JSONObject(innerValue);
//                jsonObject.keySet().forEach(key -> {
//                    if ("change_column_name".equals(key) || "change_column_type".equals(key)){
//                        JSONObject jsonObject = new JSONObject(innerValue);
//                    }
//                });
//                return innerKV;
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//
//        return null;
//    }

}
