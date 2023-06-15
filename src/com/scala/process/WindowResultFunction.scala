package process

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

class WindowResultFunction extends RichWindowFunction[Long, (String, Long, String), String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long, String)]): Unit = {
    // 获取输入数据的第一个元素 (factory, count)
    val count = input.iterator.next()
    //当前时间转为yyyy-MM-dd HH:mm:ss格式
    val currentTime = System.currentTimeMillis()
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(currentTime))
    out.collect((key, count, time))
  }

}
