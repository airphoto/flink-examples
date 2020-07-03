package com.lhs.flink.rule.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author lihuasong
 * @description 描述
 * @create 2020/6/1
 **/
public class DateUtils {

    private static final Logger logger = LoggerFactory.getLogger(DateUtils.class);

    public static String getTimeByFormat(long currentMS, String formatStr){
        try {
            SimpleDateFormat format = new SimpleDateFormat(formatStr);
            return format.format(new Date(currentMS));
        }catch (Exception e){
            logger.error("date format error",e);
        }

        return "19700101";

    }

}
