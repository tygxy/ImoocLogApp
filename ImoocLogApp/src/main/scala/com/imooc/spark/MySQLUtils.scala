package com.imooc.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by guoxingyu on 2018/1/10.
  * MySQL操作工具类
  */
object MySQLUtils {

  /**
    * 获取MySQL链接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=302313")
  }

  /**
    * 释放数据库等资源
    * @param connection
    * @param pstmt
    */
  def release(connection:Connection, pstmt:PreparedStatement) = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e : Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }
}
