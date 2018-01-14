package com.imooc.spark

import org.apache.commons.lang3.time.FastDateFormat;
import java.util.{Date, Locale}

/**
  * Created by guoxingyu on 2018/1/7.
  * 日期时间解析工具类
  */
object DateUtils {
  // 输入日期格式
  val YYYYMMDDHHMMSS_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  // 输出日期格式
  val TATGET_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取时间，格式为yyyy-mm-dd hh:mm:ss
    * @param time
    */
  def parse(time : String) = {
    TATGET_TIME_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取日志时间，转换为Long类型
    * @param time
    * @return
    */
  def getTime(time : String) = {
    try {
      YYYYMMDDHHMMSS_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e : Exception => {
        0l
      }
    }
  }
}
