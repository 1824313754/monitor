package utils

/**
 * 反射生成insert语句
 */

object SqlUtils {
  def sqlProducer[T ](tableName: String, scalaObject: T): String = {
    //获取父类的属性
    val fields = scalaObject.getClass.getDeclaredFields
    fields.foreach(_.setAccessible(true))
    val fieldsWithValues = fields.map(field => (field.getName, field.get(scalaObject)))
    val columns = fieldsWithValues.map(_._1).mkString(",")
    val values = fieldsWithValues.map { case (_, value) =>
      if (value.isInstanceOf[String]) s"'$value'" else value
    }.mkString(",")
    s"insert into $tableName ($columns) values ($values);"
  }

//  def main(args: Array[String]): Unit = {
//    val str: String = sqlProducer("ess_alarm.alarm_all", new AlarmCount())
//    print(str)
//  }


}
