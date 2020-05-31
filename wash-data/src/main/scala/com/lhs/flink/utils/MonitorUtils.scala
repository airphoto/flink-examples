package com.lhs.flink.utils
import ImplicitUtils._
object MonitorUtils {

  /**
    * key对应的指标增加
    * @param key
    * @param logMonitorMap
    */
  def monitorInc(key:String,logMonitorMap:java.util.Map[String,Integer]):Unit = {
    logMonitorMap.put(key,logMonitorMap.getOrDefault(key,0)+1)
  }

  /**
    *
    * @param logType
    * @param logMonitor
    * @param errorMesages
    */
  def errorInc(logType:String,logMonitor:java.util.Map[String,Integer],errorMesages:java.util.List[String]):Unit = {
    errorMesages.toArray().foreach(msg =>{
      val key = s"valid_error:${logType}:${msg.toString.replace("#:","").replace(" ","_")}"
      monitorInc(key,logMonitor)
      }
    )
  }

  /**
    *
    * @param logType
    * @param logMonitor
    */
  def normalInc(logType:String,logMonitor:java.util.Map[String,Integer]):Unit = {
    val currentDay = System.currentTimeMillis().long2ShortDate
    val key = s"pass:${logType}"
    monitorInc(key,logMonitor)
  }

}
