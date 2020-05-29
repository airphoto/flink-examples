package com.lhs.flink.utils

import com.alibaba.fastjson.{JSONArray, JSONObject}

import scala.collection.mutable

/**
  * 文件名称：ColumnNameRecover
  * 创建时间：2020-05-27
  * 描述：
  *
  * @author lihuasong
  * @version v1.0
  *          更新 ：[0] 2020-05-27 lihuasong [变更内容]
  **/
object ColumnNameRecover {

  /**
    * 修复列字段
    */
  private[utils] def recoveryColumn(jsonObj: JSONObject, attributeConfigMap:mutable.Map[String, mutable.Map[String, mutable.Map[String, String]]]): Unit = {
    val typeValue = jsonObj.getString("type")
    val columnsMap = attributeConfigMap.get(typeValue)
    if (columnsMap.isDefined) {
      val replaceColumnsMap = columnsMap.get.get("change_column_name")
      if (replaceColumnsMap.nonEmpty) {
        for (entry <- replaceColumnsMap.get) {
          val array = entry._1.split("\\.")
          val oldLast = array.last
          val oldPre = entry._1.replace(oldLast, "")
          val newLast = entry._2
          if (!"".equals(oldPre)) {
            repairColumns(jsonObj, jsonObj, array, 0, entry._2, isLenEq = true)
          }else{
            repairColumns(jsonObj, jsonObj, array, 0, entry._2, isLenEq = false)
          }
        }
      }
    }
  }

  /**
    * 递归取老字段的值并删除老字段以及为新字段赋值
    */
  private def repairColumns(json: JSONObject,srcJson: JSONObject,array: Array[String],index: Int,newCol: String,isLenEq: Boolean): Unit ={
    val keyTypes = array(index).split(":")
    val key = keyTypes(0)
    val keyYype = keyTypes(1)
    val jsonType = srcJson.getString("type")
    if(index == (array.length -1)){
      val value = json.get(key)
      if(isLenEq){
        if(value != null){
          json.remove(key)
          val newKey = newCol.split("\\.").last.split(":")(0)
          json.put(newKey,value)
          val message = "repair|"+jsonType+"|"+key+"|"+newKey
          //          acc.add(message)
        }
      }else{
        if(value != null){
          json.remove(key)
          val newArray = newCol.split("\\.")
          setJsonKeyValue(srcJson,newArray,0,value,key,jsonType)
        }
      }
    }else{
      val anyRef = json.get(key)
      if(anyRef != null){
        val className = anyRef.getClass.toString
        if("struct".equals(keyYype)){
          if(className.endsWith("JSONObject")){
            val jsonObj = json.getJSONObject(key)
            repairColumns(jsonObj,srcJson,array,index+1,newCol,isLenEq)
          }
        }else if("array[struct]".equals(keyYype)){
          if(className.endsWith("JSONArray")){
            val jSONArray = json.getJSONArray(key)
            val len = jSONArray.size()
            for(i <- 0 until len){
              val clName = jSONArray.get(i).getClass.toString
              if(clName.endsWith("JSONObject")){
                val jsonObj = jSONArray.getJSONObject(i)
                repairColumns(jsonObj,srcJson,array,index+1,newCol,isLenEq)
              }
            }
          }
        }
      }
    }
  }

  /**
    * 为新字段赋值
    */
  private[utils] def setJsonKeyValue(json: JSONObject,array: Array[String],index: Int,value: AnyRef,oldKey: String,jsonType: String): Unit ={
    val keyTypes = array(index).split(":")
    val len = array.length
    if(len >= 2){
      val key = keyTypes(0)
      val keyYype = keyTypes(1)
      val sign = len - 2
      if(index == sign){
        val opt = json.get(key)
        val ky = array.last.split(":")(0)
        if(opt != null){
          val className = opt.getClass.toString
          if("struct".equals(keyYype)){
            if(className.endsWith("JSONObject")){
              val jObj = json.getJSONObject(key)
              val kValue = jObj.get(ky)
              if(kValue == null){
                jObj.put(ky,value)
                val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
                //                acc.add(message)
              }else{
                val message = "repair|"+jsonType+"|"+oldKey+"|oldAndNewKeyAllExistAndDeleteOldKey"
                //                acc.add(message)
              }
            }
          }else{
            if(className.endsWith("JSONArray")){
              val jArray = json.getJSONArray(key)
              val size = jArray.size()
              if(size != 0){
                for(i <- 0 until size){
                  val cName = jArray.get(i).getClass.toString
                  if(cName.endsWith("JSONObject")){
                    val jObj = jArray.getJSONObject(i)
                    val kValue = jObj.get(ky)
                    if(kValue == null){
                      jObj.put(ky,value)
                      val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
                      //                      acc.add(message)
                    }
                  }
                }
              }else{
                val obj = new JSONObject()
                obj.put(ky,value)
                jArray.add(obj)
                val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
                //                acc.add(message)
              }
            }
          }
        }else{
          if("struct".equals(keyYype)){
            val obj = new JSONObject()
            obj.put(ky,value)
            json.put(key,obj)
            val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
            //            acc.add(message)
          }else{
            val jArray = new JSONArray()
            val obj = new JSONObject()
            obj.put(ky,value)
            jArray.add(obj)
            json.put(key,jArray)
            val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
            //            acc.add(message)
          }
        }
      }else if(index < sign){
        val anyRef = json.get(key)
        if(anyRef != null){
          val className = anyRef.getClass.toString
          if("struct".equals(keyYype)){
            if(className.endsWith("JSONObject")){
              val jsonObj = json.getJSONObject(key)
              setJsonKeyValue(jsonObj,array,index+1,value,oldKey,jsonType)
            }
          }else if("array[struct]".equals(keyYype)){
            if(className.endsWith("JSONArray")){
              val jSONArray = json.getJSONArray(key)
              val len = jSONArray.size()
              if(len != 0){
                for(i <- 0 until len){
                  val cName = jSONArray.get(i).getClass.toString
                  if(cName.endsWith("JSONObject")){
                    val jsonObj = jSONArray.getJSONObject(i)
                    setJsonKeyValue(jsonObj,array,index+1,value,oldKey,jsonType)
                  }
                }
              }else{
                val obj = new JSONObject()
                jSONArray.add(obj)
                setJsonKeyValue(obj,array,index+1,value,oldKey,jsonType)
              }
            }
          }
        }else{
          if("struct".equals(keyYype)){
            val obj = new JSONObject()
            json.put(key,obj)
            setJsonKeyValue(obj,array,index+1,value,oldKey,jsonType)
          }else if("array[struct]".equals(keyYype)){
            val arrayObj = new JSONArray()
            val obj = new JSONObject()
            arrayObj.add(obj)
            json.put(key,arrayObj)
            setJsonKeyValue(obj,array,index+1,value,oldKey,jsonType)
          }
        }
      }
    }else{
      val ky = array(0).split(":")(0)
      val kValue = json.get(ky)
      if(kValue == null){
        json.put(ky,value)
        val message = "repair|"+jsonType+"|"+oldKey+"|"+ky
        //        acc.add(message)
      }
    }
  }

}
