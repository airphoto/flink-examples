package com.lhs.flink.utils

import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, Pipeline}

import scala.collection.JavaConversions._
import ImplicitUtils._
/**
  * 描述
  * redis 辅助类
  *
  * @author lihuasong
  *
  *         2019-03-21 14:03
  **/
object RedisHelper extends Serializable {

  private val logger = LoggerFactory.getLogger("RedisHelper")

  @transient private var pools:JedisPool = _

  def makePool(properties: Properties): Unit = {
    if (pools==null) {
      try {
        val redisHost = properties.getOrElse("metric.redis.host", "")
        val redisPort = properties.getOrElse("metric.redis.port", "6379").toInt
        val redisTimeout = properties.getOrElse("metric.redis.timeout", "5000").toInt
        val passwd = properties.getOrElse("metric.redis.password", "")
        val database = properties.getOrElse("metric.redis.db", "1").toInt

        val poolConfig = new GenericObjectPoolConfig()
        poolConfig.setMaxTotal(properties.getOrElse("metric.redis.maxTotal", "500").toInt)
        poolConfig.setMaxIdle(properties.getOrElse("metric.redis.maxIdle", "50").toInt)
        poolConfig.setMinIdle(properties.getOrElse("metric.redis.minIdle", "10").toInt)
        poolConfig.setTestOnBorrow(properties.getOrElse("metric.redis.testOnBorrow", "true").toBoolean)
        poolConfig.setTestOnReturn(properties.getOrElse("metric.redis.testOnReturn", "true").toBoolean)
        poolConfig.setMaxWaitMillis(properties.getOrElse("metric.redis.maxWaitMillis", "10000").toLong)

        pools = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout, passwd, database)
        logger.info("redis pool init")
      }catch {
        case e:Exception => logger.error("redis pool init error",e)
      }
    }
  }

  def getPool : JedisPool = pools

  def getJedis : Jedis = this.getPool.getResource

  def returnSource(redis: Jedis, pipeline: Pipeline): Unit = {

    try {
      if (pipeline != null) {
        pipeline.sync()
        pipeline.close()
      }
    } catch {
      case e: Exception => logger.error("pipeline close error",e)
    } finally {
      try {
        if (redis != null) {
          redis.close()
        }
      }catch {
        case e:Exception => logger.error("redis close error",e)
      }
    }
  }

  def saveMetricData(pipeline: Pipeline,metricData:java.util.Map[String,Integer],taskName:String):Unit = {
    try{
      val metricKey = s"wash:metric:${System.currentTimeMillis().long2ShortDate}"
      metricData.foreach(kv => {
        pipeline.hset(metricKey,kv._1,kv._2.toString)
        pipeline.expire(metricKey,3600 * 19)
      })
    }catch {
      case e:Exception => logger.info("metric save error",e)
    }
  }
}
