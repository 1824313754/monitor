import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature

// 定义VehicleData类
case class VehicleData(vehicleFactory: String, vin: String, ctime: Long, nowTime: Long, sourceType: String)

object Main extends App {
  // 创建VehicleData对象列表
  val vehicleDataList: List[VehicleData] = List(
    VehicleData("Factory1", "VIN1", 1234567890L, 9876543210L, "Type1"),
    VehicleData("Factory1", "VIN1", 1234567890L, 9876543210L, "Type2"),
    VehicleData("Factory2", "VIN2", 2345678901L, 8765432109L, "Type2"),
    VehicleData("Factory3", "VIN3", 3456789012L, 7654321098L, "Type3")
  )

  // 创建最终结果的JSONObject
  val resultJson: JSONObject = new JSONObject()

  // 创建Map来存储数据
  val dataMap: collection.mutable.Map[String, collection.mutable.Map[String, JSONObject]] = collection.mutable.Map()

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

    val json: JSONObject = new JSONObject()
    json.put("vehicleFactory", vehicleData.vehicleFactory)
    json.put("vin", vehicleData.vin)
    json.put("ctime", vehicleData.ctime)
    json.put("nowTime", vehicleData.nowTime)
    json.put("sourceType", vehicleData.sourceType)

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

  // 将最终结果的JSONObject转换为JSON字符串，并使用fastjson的格式化功能
  val resultJsonString: String = JSON.toJSONString(resultJson, SerializerFeature.PrettyFormat)
  println(resultJsonString)
}
