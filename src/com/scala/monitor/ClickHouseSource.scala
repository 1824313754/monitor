package monitor

import bean.VehicleData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import ru.yandex.clickhouse.settings.ClickHouseProperties
import ru.yandex.clickhouse.{BalancedClickhouseDataSource, ClickHouseConnection}

import java.util

class ClickHouseSource(connInfo: String, username: String, password: String,tableSeconds:Int) extends RichSourceFunction[util.ArrayList[VehicleData]] {
  private var connection: ClickHouseConnection = _
  override def open(parameters: Configuration): Unit = {
    val clickPro = new ClickHouseProperties()
    clickPro.setUser(username)
    clickPro.setPassword(password)
    val source = new BalancedClickhouseDataSource(connInfo, clickPro)
    source.actualize()
    connection = source.getConnection
  }

  override def run(ctx: SourceFunction.SourceContext[util.ArrayList[VehicleData]]): Unit = {
    while (true) {
      val monitor = new TableMonitor
      val list = monitor.tableMonitor(connection)
      ctx.collect(list)
      println(tableSeconds)
      Thread.sleep(tableSeconds)
    }
  }

  override def cancel(): Unit = {
  }

  override def close(): Unit = {
    connection.close()
  }
}

