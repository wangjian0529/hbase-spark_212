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
    </dependencies>
```
# 数据源中的属性
|-|-|
|属性名|描述|
|-|-|

