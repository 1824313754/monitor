package utils

import com.alibaba.fastjson.{JSON, JSONObject}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ArrayBuffer

object CommonFuncs {


  def mkctime (year:Int,month:Int,day:Int,hours:Int,minutes:Int,seconds:Int) :Long ={
    //println("year:"+year+",month:"+month+",day:"+day+",hours:"+hours+",minutes:"+minutes+",seconds"+seconds);
    try {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("20%02d-%02d-%02d %02d:%02d:%02d".format(year, month, day, hours, minutes, seconds)).getTime / 1000
    }catch {
      case e:Throwable=> return 0;
    }
  }


  def calculateTimeDifference(ctime: String, processTime: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val startDateTime = LocalDateTime.parse(ctime, formatter)
    val endDateTime = LocalDateTime.parse(processTime, formatter)
    val minutes: Long = ChronoUnit.MINUTES.between(startDateTime, endDateTime)
    minutes
  }






}
