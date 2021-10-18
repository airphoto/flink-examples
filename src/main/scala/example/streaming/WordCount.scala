package example.streaming

import java.util
import java.util.Properties

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.fs.CloseableRegistry
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.query.TaskKvStateRegistry
import org.apache.flink.runtime.state.{KeyGroupRange, KeyedStateHandle, OperatorStateHandle, StateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.ttl.TtlTimeProvider
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010


/**
  * 描述
  *
  * @author lihuasong
  *
  *         2019-07-24 19:39
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(1000L)
    val backen = new FsStateBackend("file:\\E:\\idea\\checkpoint")
    env.setStateBackend(new StateBackend() {
      override def createCheckpointStorage(jobID: JobID) = ???

      override def createKeyedStateBackend[K](environment: Environment, jobID: JobID, s: String, typeSerializer: TypeSerializer[K], i: Int, keyGroupRange: KeyGroupRange, taskKvStateRegistry: TaskKvStateRegistry, ttlTimeProvider: TtlTimeProvider, metricGroup: MetricGroup, collection: util.Collection[KeyedStateHandle], closeableRegistry: CloseableRegistry) = ???

      override def createOperatorStateBackend(environment: Environment, s: String, collection: util.Collection[OperatorStateHandle], closeableRegistry: CloseableRegistry) = ???

      override def resolveCheckpoint(s: String) = ???
    })



    val properties = new Properties()
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("group.id","flink_test")

    val consumer = new FlinkKafkaConsumer010[String]("canal",new SimpleStringSchema(),properties)

    val stream = env.addSource(consumer)

    stream.map((_,2)).keyBy(0).sum(1).print()

    env.execute("word-count")
  }
}
