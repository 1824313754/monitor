
import bean.{ClickhouseBean, VehicleData}
import com.alibaba.fastjson.{JSON, JSONObject}
import monitor.{DstreamMonitor, TableMonitor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import sink.{ClickHouseSink, RedisSink}
import utils.{CommonFuncs, GetConfig}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object MonitorStreaming {
  def main(args: Array[String]): Unit = {
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val fileName: String = tool.get("config_path")
    val params: ParameterTool = GetConfig.getProperties(fileName)
    val delayTime = params.getInt("delay.time")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //TODO 数据接入流监控
    val dstreamMonitor = new DstreamMonitor
    val dstreamMonitorvalue: DataStream[ClickhouseBean] = dstreamMonitor.monitor(params, env)
    .map(line => {
      val vehiclefactory = line._1
      val count = line._2
      val ctime = line._3
      val json = new JSONObject()
      val nObject = new JSONObject()
      json.put("vehiclefactory", vehiclefactory)
      json.put("count", count)
      json.put("ctime", ctime)
      nObject.put("monitoring_type", "dataInputMonitor")
      nObject.put("monitoring_values", json.toString)
      nObject.put("day_of_year", ctime.substring(0, 10))
      nObject.put("processTime", ctime)
      val clickhouseBean = nObject.toJavaObject(classOf[ClickhouseBean])
      clickhouseBean
    })
    dstreamMonitorvalue.addSink(new ClickHouseSink(params))

    //TODO 数据表监控
    val tableMonitor = new TableMonitor
    val tableMonitorValue: DataStream[(String,String)] = tableMonitor.monitor(params, env).map(
      lineList=>{
        val json = convertListToJson(lineList.toList,delayTime)
        ("tableMonitor",json.toString)
      }
    )
    tableMonitorValue.map(line=>{
      val json = new JSONObject()
      val nObject = JSON.parseObject(line._2)
      json.put("monitoring_type",line._1)
      json.put("monitoring_values",nObject.toJSONString)
      json.put("day_of_year",nObject.getString("processTime").substring(0,10))
      json.put("processTime",nObject.getString("processTime"))
      val clickhouseBean = json.toJavaObject(classOf[ClickhouseBean])
      clickhouseBean
    }).addSink(new ClickHouseSink(params))
    tableMonitorValue.addSink(new RedisSink(params))
    env.execute("Flink Monitor Streaming")
  }

  /**
   * 将VehicleData对象列表转换为JSONObject
   * @param vehicleDataList
   * @param delayTime
   * @return
   */
  def convertListToJson(vehicleDataList: List[VehicleData],delayTime:Int): JSONObject = {
    // 创建最终结果的JSONObject
    val resultJson: JSONObject = new JSONObject()
    // 创建Map来存储数据
    val dataMap: collection.mutable.Map[String, collection.mutable.Map[String, JSONObject]] = collection.mutable.Map()
    var processTime = ""
    // 遍历VehicleData对象列表
    for (vehicleData <- vehicleDataList) {
      // 检查数据Map中是否存在vehicleFactory的键
      if (!dataMap.contains(vehicleData.vehicleFactory)) {
        dataMap.put(vehicleData.vehicleFactory, collection.mutable.Map())
      }

      // 检查数据Map中是否存在sourceType的键
      if (!dataMap(vehicleData.vehicleFactory).contains(vehicleData.sourceType)) {
        dataMap(vehicleData.vehicleFactory).put(vehicleData.sourceType, new JSONObject())
      }
      var delayed=0
      val json: JSONObject = new JSONObject()
      json.put("vehicleFactory", vehicleData.vehicleFactory)
      json.put("ctime", vehicleData.ctime)
      processTime= vehicleData.nowTime
      json.put("sourceType", vehicleData.sourceType)
      val diffMinutes = CommonFuncs.calculateTimeDifference(vehicleData.getCtime, vehicleData.getNowTime)
      if(diffMinutes>=delayTime){
        delayed=1
      }
      json.put("diffMinutes",diffMinutes)
      json.put("delayed",delayed)
      // 将JSONObject添加到数据Map中
      dataMap(vehicleData.vehicleFactory)(vehicleData.sourceType) = json
    }

    // 将数据Map转换为最终结果的JSONObject
    for ((vehicleFactory, nestedMap) <- dataMap) {
      val nestedJson: JSONObject = new JSONObject()
      for ((sourceType, json) <- nestedMap) {
        nestedJson.put(sourceType, json)
      }
      resultJson.put(vehicleFactory, nestedJson)
    }
    resultJson.put("processTime", processTime)
    resultJson
  }
}
