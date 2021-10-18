package example.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 描述
  *
  * @author lihuasong
  *
  *         2019-07-24 19:21
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)

    val text = if(params.has("input")){
      env.readTextFile(params.get("input"))
    }else{
      env.fromCollection(Array("test test","test word"))
    }

    val counts = text.flatMap(line=>{
      line.split(" ")
    }).map((_,1))
      .groupBy(0)
      .sum(1)

    if(params.has("output")){
      counts.writeAsCsv(params.get("output"),"\t"," ",WriteMode.OVERWRITE)
      env.execute(" batch world count")
    }else{
      counts.print()
    }
  }
}
