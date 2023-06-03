# hbase-spark_212
# 下载target目录下的依赖包，添加到本地仓库
```
mvn install:install-file "-Dfile=hbase-spark_2.12-1.0.jar" "-Dpackaging=jar" "-DgroupId=com.inspur.spark.hbase" "-DartifactId=hbase-spark_2.12" "-Dversion=1.0"
```
# 在自己的项目中添加spark-sql和hbase相关依赖
```
    <properties>
        <spark.version>3.0.0</spark.version>
        <hbase.version>2.1.8</hbase.version>
        <jackson.version>2.12.3</jackson.version>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>${hbase.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>${hbase.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-mapreduce</artifactId>
            <version>${hbase.version}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
            <version>${jackson.version}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <version>${jackson.version}</version>
        </dependency>
        <dependency>
            <groupId>com.inspur.spark.hbase</groupId>
            <artifactId>hbase-spark_2.12</artifactId>
            <version>1.0</version>
        </dependency>
    </dependencies>
```
# 数据源中的属性
|属性名|描述|
|hdfsurl|将DataFrame存储到HBase时，指定HDFS的URL地址（hdfs://host:port）,如hdfs://hadoop:9000|
|zkurl|读写HBase时，指定ZooKeeper的URL(host:port),如hadoop:2181|
|htable|读或写的HBase的表名|
|family|将DataFrame存储到HBase时，以字符串格式指定列族，如果有多个列族则通过逗号分隔，如有info和other两个列族"info,other"|
|schema|读写HBase时指定DataFrame的Schema信息("id string,列族_列 数据类型,....."),如"id string,info_name string,other_age long"|
# 示例代码
```
 val spark=SparkSession.builder().master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("HBaseTest").getOrCreate()
 //DataFrame存入HBase
 val peopleDF: DataFrame = spark.read.json("source/people.json")
    peopleDF.write
      .format("hbase")
      .mode("overwrite")
      .option("hdfsurl","hdfs://hadoop:9000")
      .option("htable","people")  
      .option("zkurl","hadoop:2181") 
      .option("family","info,other") 
      .option("schema","id string,other_sex string,info_name string,info_age long")  
      .save()
   //HBase中读取DataFrame   
   val dfs: DataFrame = spark.read.format("hbase")
      .option("zkurl", "hadoop:2181")
      .option("htable", "people")
      .option("schema", "id string,other_sex string,info_name string,info_age long")
      .load()
    dfs.show()
```
