package utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object ConvertUtil {
  // 2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("courseType", StringType),
      StructField("courseId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  def parseLong(log: String) = {
    try {
      val splits = log.split("\t")
      val url = splits(1)
      val courseTypeId = url.replace("//","/").split("/")
      val courseType = courseTypeId(2)
      val courseId = courseTypeId(3).toLong
      val traffic = splits(2).toLong
      val ip = splits(3)
      val city = IpUtil.getCity(ip)
      val time = splits(0).split(" ")(1)
      val day = splits(0).split(" ")(0)
      Row(url, courseType, courseId, traffic, ip, city, time, day)
      // http://www.imooc.com/article/17891  article  17891  407  218.75.35.226  北京  08:07:35  2017-05-11
    } catch {
      case e:Exception => Row(0)
    }

  }

}
