package monitor

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

class ClickHouseSource(connInfo: String, username: String, password: String) extends RichSourceFunction[util.ArrayList[(String, String, String,String)]] {
  private var connection: ClickHouseConnection = _

  override def open(parameters: Configuration): Unit = {
    val clickPro = new ClickHouseProperties()
    clickPro.setUser(username)
    clickPro.setPassword(password)
    val source = new BalancedClickhouseDataSource(connInfo, clickPro)
    source.actualize()
    connection = source.getConnection
  }

  override def run(ctx: SourceFunction.SourceContext[util.ArrayList[(String, String, String,String)]]): Unit = {
    while (true) {
      val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      val currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      val sql =
        s"""
           |SELECT vehicleFactory,max(ctime) as maxctime, 'ods' AS source_type,'$currentTime' AS currentTime
           |FROM source_gx.ods_all
           |WHERE day_of_year = '$currentDate' AND ctime <= '$currentTime'
           |GROUP BY vehicleFactory,source_type,currentTime
           |UNION ALL
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
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)
      //定义一个list
      val list = new util.ArrayList[(String, String, String,String)]()
      while (resultSet.next()) {
        val vehicleFactory = resultSet.getString("vehicleFactory")
        val ctime = resultSet.getString("maxctime")
        val source_type = resultSet.getString("source_type")
        val currentTime = resultSet.getString("currentTime")
        list.add(vehicleFactory, ctime, source_type,currentTime)
      }
      ctx.collect(list)
      Thread.sleep(60000)
    }
  }

  override def cancel(): Unit = {
  }

  override def close(): Unit = {
    connection.close()
  }
}

