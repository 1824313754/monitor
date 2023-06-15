package process

import bean.VehicleData
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @ClassName: VinCountAggregator
 * @Description: TODO 用于统计每个窗口每个厂家的车辆数
 */
class VinCountAggregator extends AggregateFunction[VehicleData, Set[String], Long] {
  override def createAccumulator(): Set[String] = Set[String]()

  override def add(value: VehicleData, accumulator: Set[String]): Set[String] = {
    accumulator + value.vin
  }

  override def getResult(accumulator:Set[String]):  Long = {
    accumulator.size
  }
  override def merge(a:Set[String], b:Set[String]): Set[String] = {
    a ++ b
  }

}