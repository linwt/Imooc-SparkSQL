package main

import utils.ConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

object CleanYarn {

  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("Usage: CleanYARN <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath, outputPath) = args
    val spark = SparkSession.builder().getOrCreate()
    val accessRDD = spark.sparkContext.textFile(inputPath)
    val df = spark.createDataFrame(accessRDD.map(line => ConvertUtil.parseLong(line)), ConvertUtil.struct)
    df.coalesce(1).write.format("parquet").partitionBy("day").mode(SaveMode.Overwrite).save(outputPath)
    spark.stop()
  }

}
