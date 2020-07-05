package com.lhs.flink.rule.process;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassNameJsonLogFilter
 * @Description
 *      判断日志是否是json格式
 * @Author lihuasong
 * @Date2020/7/5 17:18
 * @Version V1.0
 **/
public class JsonLogFilter implements FilterFunction<String> {
    private Logger logger = LoggerFactory.getLogger(JsonLogFilter.class);

    @Override
    public boolean filter(String s) throws Exception {
        try{
            JSON.parse(s);
            return true;
        }catch (Exception e){
            logger.error("log is not json {}",s,e);
            return false;
        }
    }
}
