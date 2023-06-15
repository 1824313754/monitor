//import java.util.{Date, Properties}
//import com.alibaba.fastjson.JSON
//import org.apache.flink.api.common.functions.AggregateFunction
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.RichWindowFunction
//import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.util.Collector
//
//import java.text.SimpleDateFormat
//import java.util.regex.Pattern
//
//case class VehicleData(vehicleFactory: String, vin: String, ctime: Long)
//
//object KafkaFlinkProgram {
//  def main(args: Array[String]): Unit = {
//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val bootstrapServers = params.get("kafka.bootstrap.servers", "cdh03:6667,cdh04:6667,cdh05:6667,cdh06:6667,cdh07:6667")
//    val groupId = params.get("kafka.consumer.groupid", "test06131339")
////    val autoOffsetReset = params.get("kafka.auto.offset.reset", "latest")
//    //设置不提交偏移量
//    params.get("kafka.enable.auto.commit", "false")
//    val odsPattern = Pattern.compile("ods-.*")
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //事件时间
//    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.getConfig.setGlobalJobParameters(params)
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", bootstrapServers)
//    properties.setProperty("group.id", groupId)
////    properties.setProperty("auto.offset.reset", autoOffsetReset)
//
//    val kafkaConsumer = new FlinkKafkaConsumer[String](odsPattern, new SimpleStringSchema(), properties)
//    kafkaConsumer.setStartFromLatest()
//    val stream = env.addSource(kafkaConsumer)
//
//    val result = stream
//      .map(parseJson(_))
//      .filter(filterByCtime(_))
//    //定义一个窗口，大小位10s，统计不同厂家的车辆数量
//      .keyBy(_.vehicleFactory)
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(40),Time.seconds(10)))
////      .trigger(new CustomTrigger(1))
//      .aggregate(new VinCountAggregator,new WindowResultFunction)
//
//
//
//    result.print()
//
//    env.execute("Kafka Flink Program")
//  }
//
//  def parseJson(jsonStr: String): VehicleData = {
//    val json = JSON.parseObject(jsonStr)
//    val vehicleFactory = json.getString("vehicleFactory")
//    val vin = json.getString("vin")
//    val ctime: Long = mkctime(json.getInteger("year")
//      , json.getInteger("month")
//      , json.getInteger("day")
//      , json.getInteger("hours")
//      , json.getInteger("minutes")
//      , json.getInteger("seconds"))
//    VehicleData(vehicleFactory, vin, ctime)
//  }
//
//  def filterByCtime(data: VehicleData): Boolean = {
//    val currentTime = System.currentTimeMillis()/1000
//    val low =currentTime - 20
//    data.ctime > low
//  }
//
//  class VinCountAggregator extends AggregateFunction[VehicleData, (String, Set[String]), (String, Long)] {
//    override def createAccumulator(): (String, Set[String]) = ("", Set[String]())
//
//    override def add(value: VehicleData, accumulator: (String, Set[String])): (String, Set[String]) = {
//      val (_, vins) = accumulator
//      (value.vehicleFactory, vins + value.vin)
//    }
//
//    override def getResult(accumulator: (String, Set[String])): (String, Long) = {
//      val (factory, vins) = accumulator
//      (factory, vins.size.toLong)
//    }
//
//    override def merge(a: (String, Set[String]), b: (String, Set[String])): (String, Set[String]) = {
//      val (factoryA, vinsA) = a
//      val (_, vinsB) = b
//      (factoryA, vinsA ++ vinsB)
//    }
//  }
//
//  class WindowResultFunction extends RichWindowFunction[(String, Long), (String, Long,String), String, TimeWindow] {
//
//    //定义一个值状态
//    lazy val isUpdate: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("isUpdate", classOf[Long]))
//
//    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Long,String)]): Unit = {
//      // 获取输入数据的第一个元素 (factory, count)
//      val (factory, count) = input.iterator.next()
//      // 获取当前状态值
//      val currentState = isUpdate.value()
//      //第四个窗口开始输出
//      if(currentState>=3){
//        //当前时间转为yyyy-MM-dd HH:mm:ss格式
//        val currentTime = System.currentTimeMillis()
//        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(currentTime))
//        out.collect((factory, count,time))
//      }
//      // 更新状态值
//      isUpdate.update(currentState+1)
//
//    }
//
//  }
//}
