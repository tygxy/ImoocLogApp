package com.imooc.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by guoxingyu on 2018/1/7.
  * 使用Spark完成数据清洗操作,运行在yarn上
  */
object SparkStatCleanJobYarn {

  /**
    * 清洗日志，清洗出code,article的URL
    * @param line
    * @return
    */
  def filterLog(line :String) = {
    val splits = line.split("\t")
    if (splits(1).contains("code") || splits(1).contains("article") || splits(1).contains("video")) {
        if (splits.length == 4) {
            true
        } else {
          false
        }
    } else {
      false
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usege: <inputPath> <outputPath>")
      System.exit(1)
    }

    val spark = SparkSession.builder().getOrCreate()

    val input = args(0)
    val output = args(1)
    val accessRDD = spark.sparkContext.textFile(input).filter(line => this.filterLog(line))

    // RDD => DF
    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(x => AccessConverUtil.parseLog(x)),AccessConverUtil.struct)
    val filterDF = accessDF.filter(!accessDF.col("url").contains("null"))

    filterDF.coalesce(1).write.format("parquet").partitionBy("dt").mode(SaveMode.Overwrite).save(output)
    spark.stop()
  }

}
