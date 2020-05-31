package com.lhs.flink.utils
import ImplicitUtils._
import com.lhs.flink.pojo.GaugeMonitor
object MonitorUtils {

  /**
    * key对应的指标增加
    * @param key
    * @param logMonitorMap
    */
  def monitorInc(key:String,logMonitorMap: GaugeMonitor):Unit = {
    val monitorData = logMonitorMap.getValue;
    monitorData.put(key,monitorData.getOrDefault(key,0)+1)
    logMonitorMap.setLogMonitor(monitorData)
  }

  /**
    *
    * @param logType
    * @param logMonitor
    * @param errorMesages
    */
  def errorInc(logType:String,logMonitor:GaugeMonitor,errorMesages:java.util.List[String]):Unit = {
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
  def normalInc(logType:String,logMonitor: GaugeMonitor):Unit = {
    val currentDay = System.currentTimeMillis().long2ShortDate
    val key = s"pass:${logType}"
    monitorInc(key,logMonitor)
  }

}
