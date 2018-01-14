# ImoocLogApp

## 1.使用说明
  项目已经mvn打包，主要包含两个可以跑在Yarn上的功能。
  
- 数据清洗的spart-submit
```
spark-submit \
--class com.imooc.spark.SparkStatCleanJobYarn \
--name SparkStatCleanJobYarn \
--master yarn \
--executor-memory 1G \
--num-executors 1 \
--files /Users/guoxingyu/lib/ImoocLogApp/ipDatabase.csv,/Users/guoxingyu/lib/ImoocLogApp/ipRegion.xlsx \
/Users/guoxingyu/lib/ImoocLogApp/sql-1.0-jar-with-dependencies.jar \
hdfs://localhost/imoock/input/ \
hdfs://localhost/imoock/clean/

// 需要加载Files文件，用于IP地址解析
```

- 统计数据存入Hive，MySQL的spart-submit
```
spark-submit \
--class com.imooc.spark.TopNStatJobYarn \
--name TopNStatJobYarn \
--master yarn \
--executor-memory 1G \
--num-executors 1 \
/Users/guoxingyu/lib/ImoocLogApp/sql-1.0-jar-with-dependencies.jar \
20161110
```

## 项目总结
### 2.1 pom小结
- 依赖一些必要的模块，包括sparkSQL,hive,mysql
```
<properties>
    <scala.version>2.11.8</scala.version>
    <spark.version>2.1.2</spark.version>
</properties>
<dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.11</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>commons-lang</groupId>
            <artifactId>commons-lang</artifactId>
            <version>2.5</version>
        </dependency>
        
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.17</version>
        </dependency>
</dependencies>
```
- 提交到yarn上时，加入<scope>provided</scope>，在本地开发时，要注释掉
- 提交到yarn上时，要加入以下
```
  <plugin>
      <artifactId>maven-assembly-plugin</artifactId>
      <configuration>
          <archive>
              <manifest>
                  <mainClass></mainClass>
              </manifest>
          </archive>
          <descriptorRefs>
              <descriptorRef>jar-with-dependencies</descriptorRef>
          </descriptorRefs>
      </configuration>
  </plugin>
```

### 2.2 SparkSQL功能小结
- 基础配置
```
val spark = SparkSession.builder()
    .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
    .enableHiveSupport()
    .getOrCreate()
```
- DF读写数据源
```
val DF1 = spark.read.format("parquet").load("hdfs://localhost:8020/xx/xx")
    DF2.write.format("text").mode(SaveMode.Overwrite).save("file:///xx/xx)
```
- RDD转变成DF，使用编程方式显示创建Schema，并与RDD相关联
```
// 步骤
// Create an RDD of Rows from the original RDD; 
// Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
// Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

// 读取RDD并转换成RowRDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))
  
// StructType形式创建Schema
val schemaString = "name age"
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)

// 关联rowRDD和schema
val peopleDF = spark.createDataFrame(rowRDD, schema)
```
- DF操作
  - DataFrame API
  - 创建临时表，使用SQL
  ```
  peopleDF.createOrReplaceTempView("people")
  val results = spark.sql("SELECT name FROM people")
  ```
- DF保存数据到MySQL
    - 参考TopNStatJobYarn类中的videoAccessTopNStat函数
  
- DF保存数据到Hive
    - 参考TopNStatJobYarn类中的videoAccessTopNStat_Hive函数
