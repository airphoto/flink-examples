package com.lhs.flink.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable

import scala.collection.JavaConversions._

object RecoveryData {

  /**
    * java调用的方法
    * @param jsonObj
    * @param attributeConfigMap
    */
  def recoveryJsonByAttribute(jsonObj: JSONObject, attributeConfigMap: java.util.Map[String, java.util.Map[String, java.util.Map[String, String]]]): Unit ={
    val attrs = attributeConfigMap.map{case (logType,functionAttrs)=>
      val funcMap = functionAttrs.map{case (function,props)=> (function,props.map{case (k,v)=>(k,v)})}

      (logType,funcMap)
    }
    recoveryJsonByAttribute(jsonObj,attrs)
  }

  /**
    * 根据配置参数MAP对json字符串进行修复
    *
    * @param jsonObj            json对象
    * @param attributeConfigMap 配置文件
    * @return
    */
  def recoveryJsonByAttribute(jsonObj: JSONObject, attributeConfigMap: mutable.Map[String, mutable.Map[String, mutable.Map[String, String]]]) = {
    //列名修复
    ColumnNameRecover.recoveryColumn(jsonObj, attributeConfigMap)
    //列类型修复
    ColumnTypeRecover.recoveryColumnTypes(jsonObj, attributeConfigMap)
  }

  def main(args: Array[String]): Unit = {
    val json = JSON.parseObject("{\"type\":\"gameOver\",\"appid\":10001,\"userId\":100,\"time\":\"2020-05-28 20:13:55.000\",\"properties\":[{\"playId\":1}]}")
    val changeColumnMap = new mutable.HashMap[String,String]()
    changeColumnMap.put("appid:int","properties:array[struct].appId:Int")

    val changeColumnTypeMap = new mutable.HashMap[String,String]()
    changeColumnTypeMap.put("properties:array[struct].appId:Int","double")

//    changeColumnTypeMap.put("time:Int","{\"format_type\":\"time_num_format\",\"string_length\":13}")
//    changeColumnTypeMap.put("time:Int","{\"format_type\":\"string_2_format\",\"format_string\":'yyyy-MM-dd HH:mm:ss.SSS'}")
    changeColumnTypeMap.put("time:Int","{\"format_type\":\"format_2_string\",\"format_string\":'yyyy-MM-dd HH:mm:ss.SSS',\"string_length\":20}")


    val changeColumnFunc = new mutable.HashMap[String,mutable.HashMap[String,String]]()
    changeColumnFunc.put("change_column_name",changeColumnMap)
    changeColumnFunc.put("change_column_type",changeColumnTypeMap)

    val logTypeP = new mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,String]]]()
    logTypeP.put("gameOver",changeColumnFunc)

//    recoveryJsonByAttribute(json,logTypeP)

    println(json.toString)
  }
}
