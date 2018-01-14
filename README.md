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


