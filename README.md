# ImoocLogApp

## 1.使用说明
  项目已经mvn打包，主要包含两个可以跑在Yarn上的功能。
### 1.1 数据清洗
- spart-submit
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

## 1.2 统计数据存入Hive，MySQL
- spart-submit
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

