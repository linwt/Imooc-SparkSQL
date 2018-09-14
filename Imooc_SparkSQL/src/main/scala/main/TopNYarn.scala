package main

import caseclass.{CityTop, DayTop, TrafficTop}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object TopNYarn {

  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("Usage: CleanYARN <inputPath> <day>")
      System.exit(1)
    }
    val Array(inputPath, day) = args
    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .getOrCreate()
    val df = spark.read.format("parquet").load(inputpath)
    df.printSchema()
    df.show(false)

    Dao.deleteData(day)
    dayTopN(spark, df, day)
    cityTopN(spark, df, day)
    trafficTopN(spark, df, day)

    spark.stop()
  }

  def dayTopN(spark: SparkSession, df: DataFrame, day: String) = {

    // 使用DataFrame方式统计
    import spark.implicits._
    val dayTopDF = df.filter($"day" === day && $"courseType" === "article")
      .groupBy("day", "courseId")
      .agg(count("courseId").as("times")).orderBy($"times".desc)
    dayTopDF.show(false)

    // 使用sql方式统计
    /*df.createOrReplaceTempView("clean_log")
    val dayTopDF = spark.sql("select day,courseId,count(1) as times " +
      "from clean_log " +
      "where day='2017-05-11' and courseType='article' " +
      "group by day,courseId " +
      "order by times desc")
    dayTopDF.show(false)*/

    // 将统计结果写入到mysql中
    try {
      dayTopDF.foreachPartition(partition => {
        val list = new ListBuffer[DayTop]
        partition.foreach(record => {
          val day = record.getAs[String]("day")
          val courseId = record.getAs[Long]("courseId")
          val times = record.getAs[Long]("times")
          list.append(DayTop(day,courseId,times))
        })
        Dao.insertDayTop(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def cityTopN(spark: SparkSession, df: DataFrame, day: String) = {

    // 使用DateFrame方式统计
    import spark.implicits._
    val cityTopDF = df.filter($"day" === day && $"courseType" === "article")
      .groupBy("day", "courseId", "city")
      .agg(count("courseId").as("times"))
    val top3 = cityTopDF.select(
      cityTopDF("day"),
      cityTopDF("courseId"),
      cityTopDF("city"),
      cityTopDF("times"),
      row_number().over(Window.partitionBy(cityTopDF("city")).orderBy(cityTopDF("times").desc)).as("timesRank")
    ).filter("timesRank<=3")
//    top3.show(false)

    // 将结果写入mysql
    top3.foreachPartition(partition => {
      val list = new ListBuffer[CityTop]
      partition.foreach(record => {
        val day = record.getAs[String]("day")
        val courseId = record.getAs[Long]("courseId")
        val city = record.getAs[String]("city")
        val times = record.getAs[Long]("times")
        val timesRank = record.getAs[Int]("timesRank")
        list.append(CityTop(day, courseId, city, times, timesRank))
      })
      Dao.insertCityTop(list)
    })

  }

  def trafficTopN(spark: SparkSession, df: DataFrame, day: String) = {

    // 使用DateFrame方式统计
    import spark.implicits._
    val trafficTopDF = df.filter($"day" === day && $"courseType" === "article")
      .groupBy("day","courseId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
      //.show(false)

    // 将结果写入mysql
    trafficTopDF.foreachPartition(partition => {
      val list = new ListBuffer[TrafficTop]
      partition.foreach(record => {
        val day = record.getAs[String]("day")
        val courseId = record.getAs[Long]("courseId")
        val traffics = record.getAs[Long]("traffics")
        list.append(TrafficTop(day, courseId, traffics))
      })
      Dao.insertTrafficTop(list)
    })

  }

}
