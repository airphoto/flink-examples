package com.lhs.flink.utils

import java.text.SimpleDateFormat
import java.util.Date

import ImplicitUtils.StringUtils
/**
  * 文件名称：TimeRecover
  * 创建时间：2020-05-27
  * 描述：
  *
  * @author lihuasong
  * @version v1.0
  *          更新 ：[0] 2020-05-27 lihuasong [变更内容]
  **/
object TimeRecover {

  private def formt2LongStr(str:String,formatStr:String,length:Int = 13):String = {
    val format = new SimpleDateFormat(str)
    val timeLong = format.parse(str).getTime
    length <= 13 match {
      case true => timeLong.toString.take(length)
      case false => (timeLong * math.pow(10,length-13).toLong).toString
    }
  }

  private def timeLong2Format(str:String,formatStr:String):String = {
    val format = new SimpleDateFormat(formatStr)
    str.length >=13 match {
      case true => format.format(new Date(str.take(13).str2Long))
      case false => format.format(new Date(str.str2Long * math.pow(10,13-str.length).toLong))
    }
  }

}
