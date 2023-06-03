package com.inspur.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
 * HBase数据源的工具类
 */
object HBaseDSUtils {
  def getConnection(zkurl:String):Connection={
    val configuration: Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum",zkurl)
    ConnectionFactory.createConnection(configuration)
  }
  def createHBaseTable(zkurl:String,tableName:String,familys:Array[String]):Unit={
    val connection: Connection = getConnection(zkurl)
    val admin: Admin = connection.getAdmin
    val tableDescriptorBuilder: TableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
    for(family<-familys){
      val  columnFamilyDescriptor:ColumnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build()
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor)
    }
    val tableDescriptor: TableDescriptor = tableDescriptorBuilder.build()
    admin.createTable(tableDescriptor)
  }

  def extract(dfSchemaStr:String):Array[DfSchema]={
    val strs: Array[String] = dfSchemaStr.split(",")
    val schemas: Array[DfSchema] = strs.map(ele => {
      val array: Array[String] = ele.trim.split(" ")
      DfSchema(array(0), array(1))
    })
    schemas
  }
}
case class  DfSchema(fieldName:String,fieldType:String)
