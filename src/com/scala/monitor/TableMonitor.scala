package monitor

import base.Monitor
import bean.VehicleData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.yandex.clickhouse.ClickHouseConnection

import java.sql.ResultSet
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util

class TableMonitor extends Monitor[DataStream[util.ArrayList[VehicleData]]] {
  override def monitor(params: ParameterTool, env: StreamExecutionEnvironment): DataStream[util.ArrayList[VehicleData]] = {
    val connInfo = params.get("clickhouse.conn")
    val connInfo2 = params.get("clickhouse.conn2")
    val username = params.get("clickhouse.user")
    val password = params.get("clickhouse.passwd")
    val tableSeconds= params.getInt("table.seconds")*1000
    val source = new ClickHouseSource(connInfo,connInfo2, username, password,tableSeconds)
    val value: DataStream[util.ArrayList[VehicleData]] = env.addSource(source)
    value
  }

  def tableMonitor(connection: ClickHouseConnection,connection2: ClickHouseConnection) = {
    val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val sql =
      s"""
         |SELECT vehicleFactory,max(ctime) as maxctime, 'dwd' AS source_type,'$currentTime' AS currentTime
         |FROM warehouse_gx.dwd_all
         |WHERE day_of_year = '$currentDate' AND ctime <= '$currentTime'
         |GROUP BY vehicleFactory,source_type,currentTime
         |UNION ALL
         |SELECT vehicle_factory as vehicleFactory,max(alarm_time) as maxctime, 'alarm' AS source_type,'$currentTime' AS currentTime
         |FROM battery_alarm.alarm_all
         |WHERE day_of_year = '$currentDate' AND alarm_time <= '$currentTime'
         |GROUP BY vehicle_factory,source_type,currentTime
         |""".stripMargin

    val sql2 =
      s"""
         |SELECT vehicleFactory,max(ctime) as maxctime, 'ods' AS source_type,'$currentTime' AS currentTime
         |FROM source_gx.ods_all
         |WHERE day_of_year = '$currentDate' AND ctime <= '$currentTime'
         |GROUP BY vehicleFactory,source_type,currentTime
         |""".stripMargin

    val statement = connection.createStatement()
    val statement2 = connection2.createStatement()
    var resultSet: ResultSet = null
    var resultSet2: ResultSet = null
    try {
      resultSet = statement.executeQuery(sql)
      resultSet2 = statement2.executeQuery(sql2)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    //定义一个list
    val list = new util.ArrayList[VehicleData]()
    while (resultSet.next()) {
      val vehicleData = new VehicleData
      val vehicleFactory = resultSet.getString("vehicleFactory")
      val ctime = resultSet.getString("maxctime")
      val source_type = resultSet.getString("source_type")
      val currentTime = resultSet.getString("currentTime")
      vehicleData.setVehicleFactory(vehicleFactory)
      vehicleData.setCtime(ctime)
      vehicleData.setSourceType(source_type)
      vehicleData.setNowTime(currentTime)
      list.add(vehicleData)
    }
    if (resultSet2 != null) {
      while (resultSet2.next()) {
        val vehicleData = new VehicleData
        val vehicleFactory = resultSet2.getString("vehicleFactory")
        val ctime = resultSet2.getString("maxctime")
        val source_type = resultSet2.getString("source_type")
        val currentTime = resultSet2.getString("currentTime")
        vehicleData.setVehicleFactory(vehicleFactory)
        vehicleData.setCtime(ctime)
        vehicleData.setSourceType(source_type)
        vehicleData.setNowTime(currentTime)
        list.add(vehicleData)
      }
    }
    list
  }
}