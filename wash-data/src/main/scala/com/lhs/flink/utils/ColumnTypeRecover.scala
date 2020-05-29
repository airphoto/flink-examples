package com.lhs.flink.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable
import ImplicitUtils.StringUtils

object ColumnTypeRecover {
  /**
    * 列类型修复
    */
  private[utils] def recoveryColumnTypes(jsonObj: JSONObject, attributeConfigMap: mutable.Map[String, mutable.Map[String, mutable.Map[String, String]]]): Unit = {
    val typeValue = jsonObj.getString("type")
    val columnsMap = attributeConfigMap.get(typeValue)
    if (columnsMap.isDefined) {
      val changeColumnTypeMap = columnsMap.get.get("change_column_type")
      if (changeColumnTypeMap.nonEmpty) {
        for (entry <- changeColumnTypeMap.get) {
          val array = entry._1.split("\\.")
          repairColumnTypes(jsonObj, array, 0,typeValue,entry._2)
        }
      }
    }
  }

  /**
    * 数据字段类型修复
    */
  private def repairColumnTypes(json: JSONObject,array: Array[String],index: Int,jsonType: String,changeType:String): Unit ={
    val keyTypes = array(index).split(":")
    val key = keyTypes(0)
    val keyYype = keyTypes(1)
    if(index == (array.length -1)){
      val value = json.get(key)
      changeType match {
        case "array" => base2Array(value,json,key,jsonType)
        case "string" | "int" | "long" | "boolean" | "double" => base2base(value,json,key,jsonType,changeType)
        case _ => timeRecover(value,json,key,jsonType,changeType)
      }

    }else{
      val opt = json.get(key)
      if(opt !=null){
        val className = opt.getClass.toString
        if("struct".equals(keyYype)){
          if(className.endsWith("JSONObject")){
            val jsonObj = json.getJSONObject(key)
            repairColumnTypes(jsonObj,array,index+1,jsonType,changeType)
          }
        }else if("array[struct]".equals(keyYype)){
          if(className.endsWith("JSONArray")){
            val jSONArray = json.getJSONArray(key)
            val len = jSONArray.size()
            for(i <- 0 until len){
              val cName = jSONArray.get(i).getClass.toString
              if(cName.endsWith("JSONObject")){
                val jsonObj = jSONArray.getJSONObject(i)
                repairColumnTypes(jsonObj,array,index+1,jsonType,changeType)
              }
            }
          }
        }
      }
    }
  }


  private def base2Array(value:AnyRef,json: JSONObject,key:String,jsonType:String):Unit={
    if(value != null){
      val strValue = value.toString
      try{
        if("".equals(strValue.trim)){
          val arrayValue = new JSONArray()
          json.put(key,arrayValue)
          val message = "repair|"+jsonType+"|"+key+"|array"
          //            acc.add(message)
        }else{
          val clName = value.getClass.toString
          if(!clName.endsWith("JSONArray")){
            val arrayValue = JSON.parseArray(strValue)
            json.put(key,arrayValue)
            val message = "repair|"+jsonType+"|"+key+"|array"
            //              acc.add(message)
          }
        }
      }catch {
        case ex: Exception =>
          var strNewValue = ""
          val className = value.getClass.toString
          if(className.endsWith("String")){
            strNewValue = "[\""+strValue+"\"]"
          }else{
            strNewValue = "["+strValue+"]"
          }
          val arrayValue = JSON.parseArray(strNewValue)
          json.put(key,arrayValue)
          val message = "repair|"+jsonType+"|"+key+"|array"
        //            acc.add(message)
      }
    }
  }


  private def base2base(value:AnyRef,json: JSONObject,key:String,jsonType:String,changeType:String):Unit = {
    if(value!=null){
      try {
        changeType match {
          case "string" => json.put(key, value.toString)
          case "int" => json.put(key, value.toString.toInt)
          case "long" => json.put(key, value.toString.toLong)
          case "boolean" => json.put(key, value.toString.toBoolean)
          case "double" => json.put(key, value.toString.toDouble)
          case _ => // pass
        }
      }catch {
        case _:Exception => json.remove(key)
      }
    }
  }

  private def timeRecover(value:AnyRef,json: JSONObject,key:String,jsonType:String,changeAttr:String):Unit = {
    if (value != null){
      try{
        val attr = JSON.parseObject(changeAttr)
        val formatType = attr.getString("format_type")
        val timestr = formatType match {
          case "format_2_string" => {
            val formatStr = attr.getString("format_string")
            val length = attr.getOrDefault("string_length","13").toString.toInt
            formt2String(value.toString,formatStr,length)
          }
          case "string_2_format" => {
            val formatStr = attr.getString("format_string")
            timeLong2Format(value.toString,formatStr)
          }
          case "time_num_format" => {
            val length = attr.getOrDefault("string_length","13").toString.toInt
            numFormat(value.toString,length)
          }
          case _ => ""
        }

        if (timestr != "") json.put(key,timestr)
      }catch {
        case _:Exception =>
      }
    }
  }

  /**
    * 格式化的日志转化成字符串,最高13位
    * @param str
    * @param formatStr
    * @param length
    * @return
    */
  private def formt2String(str:String,formatStr:String,length:Int = 13):String = {
    val format = new SimpleDateFormat(formatStr)
    val timeLong = format.parse(str).getTime
    timeLong.toString.take(length)
  }


  /**
    * 将数字字符串转化成 格式化的数据
    * @param str
    * @param formatStr
    * @return
    */
  private def timeLong2Format(str:String,formatStr:String):String = {
    val format = new SimpleDateFormat(formatStr)
    if(str.length >=13){
      format.format(new Date(str.take(13).str2Long))
    } else{
      format.format(new Date(str.str2Long * math.pow(10,13-str.length).toLong))
    }
  }

  /**
    * "{\"format_type\":\"time_num_format\",\"string_length\":13}"
    * 长度最高位16位
    * @param str
    * @param length
    * @return
    */
  private def numFormat(str:String,length:Int=13):String = {
    if(str.str2Long > 0 && length <= 16){
      (str.str2Long * math.pow(10,math.abs(16-str.length)).toLong).toString.take(length)
    }else{
      str
    }
  }
}
