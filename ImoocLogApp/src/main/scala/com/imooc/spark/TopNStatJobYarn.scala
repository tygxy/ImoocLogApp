package com.imooc.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


/**
  * Created by guoxingyu on 2018/1/10.
  * TopN统计作业,运行在Yarn
  */
object TopNStatJobYarn {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usege: <dt>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
        .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
        .enableHiveSupport()
        .getOrCreate()

    val dt = args(0)
    val input = "hdfs://localhost/imoock/clean/"
    val accessDF = spark.read.format("parquet").load(input)


//  删除mysql数据库中的数据
//  StatDAO.deleteData(dt)

//  按照video统计TopN,结果存储MySQL
//  videoAccessTopNStat(spark,accessDF,dt)

//  按照城市统计video课程的TopN，结果存入MySQL
//  cityAccessTopNStat(spark,accessDF,dt)

//  按照video统计TopN,结果存储hive表
    videoAccessTopNStat_Hive(spark,accessDF,dt)

    spark.stop()
  }

  /**
    * 最受欢迎的TopN课程,结果存储MySQL
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession,accessDF: DataFrame, dt:String) = {

//    使用DF方式统计
//    import spark.implicits._
//    val videoAccessTopNDF = accessDF.filter($"dt" === "20161110" && $"cmsType" === "video")
//         .groupBy("dt","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
//    videoAccessTopNDF.show(false)

    // 使用SQL方式统计
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select dt, cmsId, count(1) as times from access_logs " +
         s"where dt = '$dt' and cmsType = 'video' " +
         "group by dt,cmsId order by times desc")
    
    // 将统计结果导入MySQL
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val dt = info.getAs[String]("dt")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(dt,cmsId,times))
        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

  /**
    * 统计最受欢迎的TopN课程，存入hive表中
    * @param spark
    * @param accessDF
    * @param dt
    */
  def videoAccessTopNStat_Hive(spark: SparkSession,accessDF: DataFrame, dt:String): Unit = {
    // 使用SQL方式统计
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select dt, cmsId, count(1) as times from access_logs " +
      s"where dt = '$dt' and cmsType = 'video' " +
      "group by dt,cmsId order by times desc")

    // 将统计结果导入Hive表中
    try {
      val tableName = "day_video_topn_stat"
      val schema: Array[(String, String)] = Array(
        ("cms_id","'课程ID'"),
        ("times","'次数'")
      )
      val partitionKey: (String, String) = ("dt", "yyyyMMdd,必有项")
      val partitionValue = dt
      val location = "hdfs://localhost:8020/imoock/day_video_topn_stat"

      // DF数据按指定格式存入hdfs中
      val savaSchema = Array("cmsId","times")
      HiveUtils.DFSaveAsTextFile(spark,videoAccessTopNDF,partitionKey,partitionValue,savaSchema,location)
      // 创建hive表
      HiveUtils.createTable(spark,tableName,schema,partitionKey,partitionValue,location)
      // 将hdfs关联hive表
      HiveUtils.alterTable(spark,tableName,partitionKey,partitionValue,location)

    } catch {
      case e : Exception => e.printStackTrace()
    }
  }


  /**
    * 按城市统计最受欢迎的TopN课程,结果存储MySQL
    * @param spark
    * @param accessDF
    */

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, dt:String) = {
    accessDF.createOrReplaceTempView("access_logs")
    val str_sql =
      s"""
         select *
         from (
            select
              dt,cmsId,city,times,rank() over (partition by city order by times desc) as times_rank
             from (
                select
                  dt,cmsId,city,count(1) as times
                from access_logs
                where dt = '$dt' and cmsType = 'video'
                group by dt,city,cmsid
              ) t1
          ) t2
          where times_rank <= 5
      """
    val cityAccessTopNDF = spark.sql(str_sql)

    // 将统计结果导入MySQL
    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val dt = info.getAs[String]("dt")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(dt,cmsId,city,times,times_rank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

}
