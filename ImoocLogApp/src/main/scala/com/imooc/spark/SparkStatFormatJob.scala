package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by guoxingyu on 2018/1/7.
  * 完成日志第一步清洗：抽出指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val filepath = "file:///Users/guoxingyu/Documents/work/spark/sql/ImoocLogApp/source/access_10000.log"
    val output = "file:///Users/guoxingyu/Documents/work/spark/sql/ImoocLogApp/output/"
    val access = spark.sparkContext.textFile(filepath)

    access.map(line => {
      val split = line.split(" ")
      val ip = split(0)
      val time = split(3)+" "+split(4)
      val url = split(11).replace("\"","")
      val traffic = split(9)
      DateUtils.parse(time) + "\t"+ url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile(output)

    spark.stop()
  }
}
