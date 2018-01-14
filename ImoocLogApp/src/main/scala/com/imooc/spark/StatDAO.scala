package com.imooc.spark

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by guoxingyu on 2018/1/10.
  * 各个维度的统计DAO操作
  */
object StatDAO {
  /**
    * 批量保存DayVideoAccessStat到数据库
    * @param list
    */
  def insertDayVideoAccessTopN (list: ListBuffer[DayVideoAccessStat]) = {

    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      val sql = "insert into day_video_topn_stat(dt,cms_id,times) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      connection.setAutoCommit(false) // 设置手动提交

      for (ele <- list) {
        pstmt.setString(1, ele.dt)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection,pstmt)
    }
  }

  /**
    * 批量保存DayCityVideoAccessStat到数据库
    * @param list
    */
  def insertDayCityVideoAccessTopN (list: ListBuffer[DayCityVideoAccessStat]) = {

    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      val sql = "insert into day_city_video_topn_stat(dt,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      connection.setAutoCommit(false) // 设置手动提交

      for (ele <- list) {
        pstmt.setString(1, ele.dt)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5,ele.times_rank)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection,pstmt)
    }
  }

  /**
    * 删除当天的MySQL数据库数据
    * @param dt
    */
  def deleteData (dt:String) = {
    val tables = Array("day_city_video_topn_stat","day_video_topn_stat")

    var connection : Connection = null
    var pstmt : PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      for (table <- tables) {
        var deleteSQL = s"delete from $table where dt = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1,dt)
        pstmt.executeUpdate()
      }
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection,pstmt)
    }
  }
}
