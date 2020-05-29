package com.lhs.flink.utils

/**
  * 文件名称：StringUtils
  * 创建时间：2020-05-27
  * 描述：
  *
  * @author lihuasong
  * @version v1.0
  *          更新 ：[0] 2020-05-27 lihuasong [变更内容]
  **/
object ImplicitUtils {
    implicit class StringUtils(string:String) {
        def str2Long:Long = {
            try {
                string.toLong
            }catch {
                case _:Exception => 0L
            }
        }
    }
}
