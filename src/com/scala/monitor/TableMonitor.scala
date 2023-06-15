package monitor

import base.Monitor
import bean.VehicleData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

class TableMonitor extends Monitor[DataStream[util.ArrayList[VehicleData]]]{
  override def monitor(params: ParameterTool, env: StreamExecutionEnvironment): DataStream[util.ArrayList[VehicleData]] = {
      val connInfo = params.get("clickhouse.conn")
      val username = params.get("clickhouse.user")
      val password = params.get("clickhouse.passwd")

      val source = new ClickHouseSource(connInfo, username, password)
      val value: DataStream[util.ArrayList[(String, String, String,String)]] = env.addSource(source)
      val vehicleData = value.map(line => {
        val vehicleDataList = new util.ArrayList[VehicleData]()
        //循环遍历每一行数据
        for (i <- 0 until line.size()) {
          val data = new VehicleData
          data.setVehicleFactory(line.get(i)._1)
          data.setCtime(line.get(i)._2)
          data.setNowTime(line.get(i)._4)
          data.setSourceType(line.get(i)._3)
          vehicleDataList.add(data)
        }
        vehicleDataList
      })
      vehicleData
  }
}
