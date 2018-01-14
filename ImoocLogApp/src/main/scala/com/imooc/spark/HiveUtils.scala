package com.imooc.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by guoxingyu on 2018/1/12.
  */
object HiveUtils {

  /**
    * 创建hive表
    * @param table_name
    * @param schema
    * @param partitionKey
    * @param partitionValue
    * @param location
    */
  def createTable(spark:SparkSession,table_name:String, schema:Array[(String, String)], partitionKey:(String, String),
                  partitionValue:String, location:String) = {
    val newschema: Array[String] = schema.map(line => line._1 + " STRING,")
    var schemaStr = ""
    for (ele <- newschema) {
      schemaStr += ele
    }
    schemaStr = schemaStr.substring(0,schemaStr.length - 1)
    val partitionKeyStr = partitionKey._1 + " STRING "
    val create_table_cmd = s"CREATE EXTERNAL TABLE IF NOT EXISTS $table_name ($schemaStr)  " +
      s"PARTITIONED BY ($partitionKeyStr)  ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','  LOCATION '$location'"
    spark.sql(create_table_cmd)
 }

  /**
    * DF数据转换格式后存HDFS中
    * @param spark
    * @param DF
    * @param partitionKey
    * @param schema
    * @param location
    */
  def DFSaveAsTextFile(spark: SparkSession,DF: DataFrame,partitionKey:(String,String),partitionValue:String,
                       schema:Array[String],location:String) = {
    // 获取partition字段
    val partition = partitionKey._1
    // 拼接字符串
    val newschema: Array[String] = schema.map(line => line + ",',',")
    var schemaStr = ""
    for (ele <- newschema) {
      schemaStr += ele
    }
    schemaStr = schemaStr + partition
    // 使用SQL构造出存入到HDFS的指定格式
    DF.createOrReplaceTempView("tempTable")
    val sql_str =
      s"""
        select concat($schemaStr) as strings from tempTable
      """
    // 存储到HDFS
    val newlocation = location + s"/$partition=$partitionValue"
    spark.sql(sql_str).write.format("text").mode(SaveMode.Overwrite).save(newlocation)
  }

  /**
    * HDFS的分区数据关联到hive表中
    * @param spark
    * @param table_name
    * @param partitionKey
    * @param partitionValue
    */
  def alterTable(spark:SparkSession,table_name:String,partitionKey:(String, String),partitionValue:String,location:String) = {
    // 关联
    val partition = partitionKey._1
    val alter_table_cmd = s"alter table $table_name add partition ($partition = '$partitionValue') location " +
        s"'$location/$partition=$partitionValue'"
    spark.sql(alter_table_cmd)
  }
}
