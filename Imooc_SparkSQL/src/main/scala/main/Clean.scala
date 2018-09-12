package main

import utils.ConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

object Clean {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("CleanJob").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("E:/ImoocData/format")
    accessRDD.take(10).foreach(println)
    // 2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
    val df = spark.createDataFrame(accessRDD.map(line => ConvertUtil.parseLong(line)), ConvertUtil.struct)
    df.printSchema()
    df.show(false)
    // http://www.imooc.com/article/17891  article  17891  407  218.75.35.226  北京  08:07:35  2017-05-11
    df.coalesce(1).write.format("parquet").partitionBy("day")
      .mode(SaveMode.Overwrite).save("E:/ImoocData/clean")

    spark.stop()
  }

}
