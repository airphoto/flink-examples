package com.lhs.flink.rule.utils

object ImplicitUtils {
  implicit class StringUtils(string: String){

    def str2Int:Int = {
      try {
        string.toInt
      }catch {
        case e:Exception => 0
      }
    }

  }
}
