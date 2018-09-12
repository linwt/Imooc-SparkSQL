package main

import java.sql.{Connection, PreparedStatement}
import caseclass.{CityTop, DayTop, TrafficTop}
import utils.MySQLUtil
import scala.collection.mutable.ListBuffer

object Dao {

  def insertDayTop(list: ListBuffer[DayTop]) = {
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into day_top(day,courseId,times) values(?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1, ele.day)
        state.setLong(2, ele.courseId)
        state.setLong(3, ele.times)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }

  def insertCityTop(list: ListBuffer[CityTop]) = {
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into city_top(day,courseId,city,times,timesRank) values(?,?,?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1, ele.day)
        state.setLong(2, ele.courseId)
        state.setString(3, ele.city)
        state.setLong(4, ele.times)
        state.setInt(5, ele.timesRank)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }

  def insertTrafficTop(list: ListBuffer[TrafficTop]) = {
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      con.setAutoCommit(false)
      val sql = "insert into traffic_top(day,courseId,traffics) values(?,?,?)"
      state = con.prepareStatement(sql)
      for(ele <- list) {
        state.setString(1, ele.day)
        state.setLong(2, ele.courseId)
        state.setLong(3, ele.traffics)
        state.addBatch()
      }
      state.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }

  def deleteData(day: String) = {
    val tables = Array("day_top", "city_top", "traffic_top")
    var con: Connection = null
    var state: PreparedStatement = null
    try {
      con = MySQLUtil.getConnection()
      for(table <- tables) {
        val deleteSQL = s"delete from $table where day=?"
        val state = con.prepareStatement(deleteSQL)
        state.setString(1, day)
        state.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(con, state)
    }
  }
}
