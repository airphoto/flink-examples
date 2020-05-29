package com.lhs.flink.pojo;

import java.util.Map;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/29
 **/
public class LogAttribute {
    private Map<String,String> change_column_name;
    private Map<String,String> change_column_type;

    public Map<String, String> getChange_column_name() {
        return change_column_name;
    }

    public void setChange_column_name(Map<String, String> change_column_name) {
        this.change_column_name = change_column_name;
    }

    public Map<String, String> getChange_column_type() {
        return change_column_type;
    }

    public void setChange_column_type(Map<String, String> change_column_type) {
        this.change_column_type = change_column_type;
    }
}
