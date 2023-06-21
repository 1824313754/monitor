//package sink
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import redis.clients.jedis.{JedisPool, JedisPoolConfig}
//
//class RedisSink(redisProperties: ParameterTool) extends RichSinkFunction[(String, String)] {
//
//  private var jedisPool: JedisPool = _
//
//  override def open(parameters: Configuration): Unit = {
//    val config = new JedisPoolConfig()
//    config.setMaxTotal(redisProperties.get("redis.maxCon").toInt)
//    config.setMaxIdle(redisProperties.get("redis.maxIdle").toInt)
//    config.setTestOnBorrow(true)
//    config.setTestOnReturn(true)
//
//    jedisPool = new JedisPool(
//      config,
//      redisProperties.get("redis.host"),
//      redisProperties.get("redis.port").toInt,
//      redisProperties.get("redis.timeOut").toInt,
//      redisProperties.get("redis.password")
//    )
//
//    println("Connected to Redis successfully!")
//  }
//
//  override def invoke(value: (String, String), context: SinkFunction.Context): Unit = {
//    val jedis = jedisPool.getResource
//    try {
//      jedis.select(2)
//      jedis.set(value._1, value._2)
//    } finally {
//      jedis.close()
//    }
//  }
//
//  override def close(): Unit = {
//    jedisPool.close()
//  }
//}
