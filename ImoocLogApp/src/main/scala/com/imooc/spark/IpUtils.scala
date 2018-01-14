package com.imooc.spark

import com.ggstar.util.ip.IpHelper

/**
  * Created by guoxingyu on 2018/1/10.
  * IP解析工具类
  */
object IpUtils {
  def getCity(ip : String) = {
    IpHelper.findRegionByIp(ip)
  }
}
