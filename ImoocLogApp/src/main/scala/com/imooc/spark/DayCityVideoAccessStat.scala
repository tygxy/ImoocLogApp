package com.imooc.spark

/**
  * Created by guoxingyu on 2018/1/11.
  * 按城市统计每天课程访问次数实体类
  */

case class DayCityVideoAccessStat(dt:String, cmsId:Long, city:String, times:Long, times_rank:Int)

