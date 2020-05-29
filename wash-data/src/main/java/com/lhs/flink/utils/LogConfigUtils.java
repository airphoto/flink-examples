package com.lhs.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lhs.flink.pojo.LogAttribute;
import org.apache.commons.lang3.StringUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONTokener;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/29
 **/
public class LogConfigUtils {
    public static Map<String,Schema> initSchema(String logConfigs){

        Map<String,Schema> schemaMap = new HashMap<>();

        JSONArray objects = JSON.parseArray(logConfigs);
        objects.forEach(obj->{
            JSONObject jobj = (JSONObject)obj;
            String logType = jobj.getString("logType");
            schemaMap.put(logType,getJsonSchema(jobj.getString("logSchema")));
        });

        return schemaMap;

    }

    public static Map<String,Map<String,Map<String,String>>> initRecoverAttris(String logConfigs){
        Map<String,Map<String,Map<String,String>>> recoverMap = new HashMap<>();
        JSONArray objects = JSON.parseArray(logConfigs);
        objects.forEach(obj->{
            JSONObject jobj = (JSONObject)obj;
            String logType = jobj.getString("logType");
            Map<String, Map<String, String>> columnRecover = getRecoverAttr(jobj.getString("columnRecover"));
            if(columnRecover !=null) {
                recoverMap.put(logType, columnRecover);
            }
        });
        return recoverMap;
    }

    private static Schema getJsonSchema(String schemaString){
        Schema schema = null;
        try {
            schema = SchemaLoader.load(new org.json.JSONObject(new JSONTokener(schemaString)));
        }catch (Exception e){
            e.printStackTrace();
        }
        return schema;
    }

    private static Map<String,Map<String,String>> getRecoverAttr(String columnRecoverStr){
        Map<String,Map<String,String>> functionKV = null;
        if (StringUtils.isNoneBlank(columnRecoverStr)){
            functionKV = new HashMap<>();
            LogAttribute logAttribute = JSONObject.parseObject(columnRecoverStr, LogAttribute.class);
            if (logAttribute.getChange_column_name()!=null){
                functionKV.put("change_column_name",logAttribute.getChange_column_name());
            }

            if (logAttribute.getChange_column_type()!=null){
                functionKV.put("change_column_type",logAttribute.getChange_column_type());
            }

        }

        return functionKV;
    }

    public static void main(String[] args) {
//        String str = "[{\"columnRecover\":\"{\\\"change_column_type\\\": {\\\"time:int\\\": {\\\"format_type\\\":\\\"time_num_format\\\",\\\"string_length\\\":10}}}\",\"logSchema\":\"{\\\"$schema\\\": \\\"http://json-schema.org/draft-07/schema#\\\",\\\"title\\\": \\\"gameover\\\",\\\"description\\\": \\\"Astructureforgameover\\\",\\\"type\\\": \\\"object\\\",\\\"properties\\\": {\\\"type\\\": {\\\"type\\\": \\\"string\\\"},\\\"appId\\\": {\\\"type\\\": \\\"number\\\"},\\\"userId\\\": {\\\"type\\\": \\\"number\\\"},\\\"time\\\": {\\\"type\\\": \\\"string\\\",\\\"maxLength\\\": 13,\\\"minLength\\\": 10},\\\"properties\\\": {\\\"type\\\": \\\"object\\\",\\\"properties\\\": {\\\"playId\\\": {\\\"type\\\": \\\"number\\\"}}}},\\\"required\\\": [\\\"type\\\",\\\"appId\\\",\\\"userId\\\",\\\"properties\\\"]}\",\"logSchemaValidator\":{\"description\":\"Astructureforgameover\",\"patternProperties\":{},\"propertyDependencies\":{},\"propertySchemas\":{\"appId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false},\"time\":{\"formatValidator\":{},\"maxLength\":13,\"minLength\":10},\"type\":{\"formatValidator\":{\"$ref\":\"$[0].logSchemaValidator.propertySchemas.time.formatValidator\"}},\"userId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false},\"properties\":{\"patternProperties\":{},\"propertyDependencies\":{},\"propertySchemas\":{\"playId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false}},\"requiredProperties\":[],\"schemaDependencies\":{}}},\"requiredProperties\":[\"type\",\"appId\",\"userId\",\"properties\"],\"schemaDependencies\":{},\"title\":\"gameover\"},\"logType\":\"gameover\"},{\"columnRecover\":\"\",\"logSchema\":\"{\\\"$schema\\\": \\\"http://json-schema.org/draft-07/schema#\\\",\\\"title\\\": \\\"login\\\",\\\"description\\\": \\\"Astructureforlogin\\\",\\\"type\\\": \\\"object\\\",\\\"properties\\\": {\\\"type\\\": {\\\"type\\\": \\\"string\\\"},\\\"appId\\\": {\\\"type\\\": \\\"number\\\"},\\\"userId\\\": {\\\"type\\\": \\\"number\\\"},\\\"time\\\": {\\\"type\\\": \\\"string\\\",\\\"maxLength\\\": 13,\\\"minLength\\\": 10},\\\"properties\\\": {\\\"type\\\": \\\"object\\\",\\\"properties\\\": {\\\"playId\\\": {\\\"type\\\": \\\"number\\\"}}}},\\\"required\\\": [\\\"type\\\",\\\"appId\\\",\\\"userId\\\",\\\"properties\\\"]}\",\"logSchemaValidator\":{\"description\":\"Astructureforlogin\",\"patternProperties\":{},\"propertyDependencies\":{},\"propertySchemas\":{\"appId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false},\"time\":{\"formatValidator\":{\"$ref\":\"$[0].logSchemaValidator.propertySchemas.time.formatValidator\"},\"maxLength\":13,\"minLength\":10},\"type\":{\"formatValidator\":{\"$ref\":\"$[0].logSchemaValidator.propertySchemas.time.formatValidator\"}},\"userId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false},\"properties\":{\"patternProperties\":{},\"propertyDependencies\":{},\"propertySchemas\":{\"playId\":{\"exclusiveMaximum\":false,\"exclusiveMinimum\":false}},\"requiredProperties\":[],\"schemaDependencies\":{}}},\"requiredProperties\":[\"type\",\"appId\",\"userId\",\"properties\"],\"schemaDependencies\":{},\"title\":\"login\"},\"logType\":\"login\"}]";
//        initSchemaAndAttr(str);
    }
}
