package com.imooc.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
  * Created by guoxingyu on 2018/1/7.
  * 访问日志转换工具类
  */
object AccessConverUtil {

  // 定义的输出字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("dt", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log
    */
  def parseLog(log:String) = {
    try {
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      var cms = url.substring(url.indexOf(domain) + domain.length)
      if (cms.contains("?")) {
        cms = cms.substring(0,cms.indexOf("?"))
      }
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val dt = time.substring(0,10).replaceAll("-","")

      //Row中字段要和结构体中字段对应
      Row(url,cmsType,cmsId,traffic,ip,city,time,dt)
    } catch {
      case e: Exception => Row("null","null",0l,0l,"null","null","null","null")
    }
  }

}
