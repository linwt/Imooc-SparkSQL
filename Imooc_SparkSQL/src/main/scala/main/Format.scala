package main

import utils.DateUtil
import org.apache.spark.sql.SparkSession

object Format {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("FormatSpark").getOrCreate()

    val access = spark.sparkContext.textFile("E:/ImoocData/init.log")
    access.take(10).foreach(println)
    // 218.75.35.226 - - [11/05/2017:08:07:35 +0800] "POST /api3/getadv HTTP/1.1" 200 407 "http://www.imooc.com/article/17891" "-" cid=0&timestamp=1455254555&uid=5844555
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val traffic = splits(9)
      val url = splits(10).replace("\"", "")
      DateUtil.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
      // 2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
    }).saveAsTextFile("E:/ImoocData/format")//.take(10).foreach(println)

    spark.stop()
  }
}
