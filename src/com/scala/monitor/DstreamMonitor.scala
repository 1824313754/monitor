package monitor

import base.Monitor
import bean.VehicleData
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import process.{VinCountAggregator, WindowResultFunction}
import utils.CommonFuncs.mkctime

import java.util.Properties
import scala.collection.JavaConverters.bufferAsJavaListConverter

class DstreamMonitor extends Monitor[DataStream[(String, Long, String)]]{
  override def monitor(params: ParameterTool, env: StreamExecutionEnvironment): DataStream[(String, Long, String)] = {
    val properties = new Properties()
    val bootstrapServers = params.get("kafka.bootstrap.servers")
    val groupId = params.get("kafka.consumer.groupid")
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("group.id", groupId)
    val datainput=params.getInt("datainput.seconds")
    properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 设置会话超时时间为60秒
    //读取kafka的topic列表
    val topicString = params.get("kafka.topic")
    val topicList: java.util.List[String] = topicString.split(",").toBuffer.asJava
    val kafkaConsumer = new FlinkKafkaConsumer[String](topicList, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()
    val stream = env.addSource(kafkaConsumer)
    val result = stream
      .map(parseJson(_))
//      .filter(filterByCtime(_))
      //定义一个滚动窗口，大小位60s，统计不同厂家的车辆数量
      .keyBy(_.vehicleFactory)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(datainput)))
      .aggregate(new VinCountAggregator, new WindowResultFunction)
    result
  }
  def parseJson(jsonStr: String): VehicleData = {
    val data = new VehicleData()
    val json = JSON.parseObject(jsonStr)
    val vehicleFactory = json.getString("vehicleFactory")
    val vin = json.getString("vin")
    val ctimeTimeStemp: Long = mkctime(json.getInteger("year")
      , json.getInteger("month")
      , json.getInteger("day")
      , json.getInteger("hours")
      , json.getInteger("minutes")
      , json.getInteger("seconds"))
    data.setVehicleFactory(vehicleFactory)
    data.setVin(vin)
    data.setCtimeTimeStemp(ctimeTimeStemp)
    data
  }

  //TODO 距离当前时间20s内的数据都算实时数据
  def filterByCtime(data: VehicleData): Boolean = {
    val currentTime = System.currentTimeMillis() / 1000
    val low = currentTime - 30
    data.ctimeTimeStemp > low
  }
}
