package com.imooc.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions

/**
  * Created by guoxingyu on 2018/1/7.
  * 使用Spark完成数据清洗操作
  */
object SparkStatCleanJob {

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
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val input = "file:///Users/guoxingyu/Documents/work/spark/sql/ImoocLogApp/output/part-0000*"
    val output = "file:///Users/guoxingyu/Documents/work/spark/sql/ImoocLogApp/clean/"

    val accessRDD = spark.sparkContext.textFile(input).filter(line => this.filterLog(line))

    // RDD => DF
    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(x => AccessConverUtil.parseLog(x)),AccessConverUtil.struct)
    val filterDF = accessDF.filter(!accessDF.col("url").contains("null"))

    // 保存数据
    filterDF.coalesce(1).write.partitionBy("dt").format("parquet").mode(SaveMode.Overwrite).save(output)
    spark.stop()
  }

}
