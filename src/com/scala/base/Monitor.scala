package base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

trait Monitor[T] extends Serializable {
  //定义一个监控方法，传入一个配置文件和流处理环境
  def monitor(params: ParameterTool, env: StreamExecutionEnvironment): T
}
