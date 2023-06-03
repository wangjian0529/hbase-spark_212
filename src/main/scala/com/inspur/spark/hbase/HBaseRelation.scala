package com.inspur.spark.hbase

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

private[spark] case class HBaseRelation(parameters: Map[String, String])
                                          (@transient context: SQLContext)
extends BaseRelation with InsertableRelation with TableScan {

  override def sqlContext: SQLContext = context
  private val tableName: String = parameters.getOrElse("htable",sys.error("请指定htable"))
  private val zkurl: String = parameters.getOrElse("zkurl",sys.error("请指定zkurl"))
  private val dfSchemaStr: String = parameters.getOrElse("schema",sys.error("请指定df的schema"))
  private val dfschemas: Array[DfSchema] = HBaseDSUtils.extract(dfSchemaStr)

  /**
   * 构建DataFrame的Schema
   * @return
   */
  override def schema: StructType = {
    val fields: Array[StructField] = dfschemas.map(dfschema => {
      val fieldName: String = dfschema.fieldName
      if(fieldName.equals("id")){
        StructField("id", StringType)
      }else{
        val fname:String=fieldName.split("_")(1)
        dfschema.fieldType.toLowerCase match {
          case "string" => StructField(fname, StringType)
          case "long" => StructField(fname, LongType)
          case "int" => StructField(fname, IntegerType)
          case "double" => StructField(fname, DoubleType)
        }
      }
    })
    StructType(fields)
  }

  /**
   * 写入HBase
   * @param data:写入HBase的DataFrame
   * @param overwrite
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val rdd: RDD[Row] = data.rdd
    val saveRDD: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.map(row => {
      val rk: String = row.getAs[String]("id")
      dfschemas.filter(_.fieldName != "id").map(x => {
        val fAndQ: Array[String] = x.fieldName.split("_")
        val family: String = fAndQ(0)
        val qualify: String = fAndQ(1)
        val value: String = row.getAs(qualify).toString
        val kv = new KeyValue(
          rk.getBytes(),
          family.getBytes(),
          qualify.getBytes(),
          value.getBytes()
        )
        ((rk, family, qualify), kv)
      })
    }).flatMap(x => x)
      .sortByKey()
      .map(x=>(new ImmutableBytesWritable(x._1._1.getBytes()),x._2))

    val hdfsUrl: String = parameters.getOrElse("hdfsurl",sys.error("请传递hdfs的url"))
    val path=s"${hdfsUrl}/spark_hbase/hfiles"
    val hbaseConfig: Configuration = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum",zkurl)
    hbaseConfig.set("hbase.mapreduce.hfileoutputformat.table.name",tableName)
    val fileSystem: FileSystem = FileSystem.get(new URI(hdfsUrl),hbaseConfig)
    if(fileSystem.exists(new Path(path))){
      fileSystem.delete(new Path(path),true)
    }

    saveRDD.saveAsNewAPIHadoopFile(
      path,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConfig
    )
    val loader = new LoadIncrementalHFiles(hbaseConfig)
    val connection: Connection = HBaseDSUtils.getConnection(zkurl)
    val admin: Admin = connection.getAdmin
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    val regionLocator: RegionLocator = connection.getRegionLocator(TableName.valueOf(tableName))
    loader.doBulkLoad(
      new Path(path),
      admin,
      table,
      regionLocator
    )
  }
  /**
   * 查询
   * @return
   */
  override def buildScan(): RDD[Row] = {
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum",zkurl)
    configuration.set(TableInputFormat.INPUT_TABLE,tableName)
    val readRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val rowRDD: RDD[Row] = readRDD.map(line => {
      val buffer=new ArrayBuffer[Any]()
      val result: Result = line._2
      dfschemas.map(x=>{
        val fname:String=x.fieldName
        if(fname.equals("id")){
            val rk:String=Bytes.toString(result.getRow)
            buffer+=rk
        }else{
          val familyAndQulify: Array[String] = fname.split("_")
          val family:String=familyAndQulify(0)
          val qualify:String=familyAndQulify(1)
          x.fieldType.toLowerCase match {
            case "string" =>buffer+=Bytes.toString(result.getValue(family.getBytes(),qualify.getBytes()))
            case "int" =>buffer+=Integer.parseInt(Bytes.toString(result.getValue(family.getBytes(),qualify.getBytes())))  //23
            case "long" => buffer+=java.lang.Long.parseLong(Bytes.toString(result.getValue(family.getBytes(),qualify.getBytes())))
            case "double"=> buffer+=java.lang.Double.parseDouble(Bytes.toString(result.getValue(family.getBytes(),qualify.getBytes())))
          }
        }
      })
      Row.fromSeq(buffer)
    })
    rowRDD
  }
}
