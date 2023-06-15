package sink

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool

class RedisSink(redisProperties:ParameterTool) extends RichSinkFunction[(String,String)] {
  @volatile var jedisPool: Pool[Jedis] = _
  override def open(parameters: Configuration): Unit = {
    if(jedisPool ==null){
      synchronized{
        if(jedisPool ==null){
          val config = new GenericObjectPoolConfig()
          config.setMaxTotal(redisProperties.get("redis.maxCon").toInt) //最大连接数
          config.setMaxIdle(redisProperties.get("redis.maxIdle").toInt) //最大空闲数
          jedisPool = new JedisPool(config, redisProperties.get("redis.host"),
            redisProperties.get("redis.port").toInt,
            redisProperties.get("redis.timeOut").toInt,
            redisProperties.get("redis.password"), 2)
          println("connect redis successful!")
        }
      }
    }
  }

  override def invoke(value: (String, String), context: SinkFunction.Context): Unit =
  {
    val jedis = jedisPool.getResource
    jedis.select(2)
    jedis.set(value._1, value._2)
    println(value._1, value._2)
  }

  override def close(): Unit = {
    jedisPool.close()
  }
}
