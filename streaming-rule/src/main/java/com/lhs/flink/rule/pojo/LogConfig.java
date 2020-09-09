package com.lhs.flink.rule.pojo;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/5/26
 **/
public class LogConfig {
    private int id;
    private String functionName;
    private String processJS;
    private int status;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getProcessJS() {
        return processJS;
    }

    public void setProcessJS(String processJS) {
        this.processJS = processJS;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public String toString() {
        return "LogConfig{" +
                "id=" + id +
                ", functionName='" + functionName + '\'' +
                ", processJS='" + processJS + '\'' +
                ", status=" + status +
                '}';
    }
}