package monitor

import base.Monitor
import bean.VehicleData
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import process.{VinCountAggregator, WindowResultFunction}
import utils.CommonFuncs.mkctime

import java.util.Properties
import java.util.regex.Pattern

class DstreamMonitor extends Monitor[DataStream[(String, Long, String)]]{
  override def monitor(params: ParameterTool, env: StreamExecutionEnvironment): DataStream[(String, Long, String)] = {
    val properties = new Properties()
    val bootstrapServers = params.get("kafka.bootstrap.servers")
    val groupId = params.get("kafka.consumer.groupid")
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("group.id", groupId)
    //设置不提交偏移量
    params.get("kafka.enable.auto.commit")
    val odsPattern = Pattern.compile("ods-.*")
    val kafkaConsumer = new FlinkKafkaConsumer[String](odsPattern, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromLatest()
    val stream = env.addSource(kafkaConsumer)
    val result = stream
      .map(parseJson(_))
      .filter(filterByCtime(_))
      //定义一个滚动窗口，大小位40s，统计不同厂家的车辆数量
      .keyBy(_.vehicleFactory)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
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
    val low = currentTime - 20
    data.ctimeTimeStemp > low
  }
}
