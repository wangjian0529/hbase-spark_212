package com.inspur.spark.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider
  with RelationProvider
  with DataSourceRegister{
  override def shortName(): String = "hbase"

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val relation: HBaseRelation = HBaseRelation(parameters)(sqlContext)

    val tableName: String = parameters.getOrElse("htable",sys.error("请指定正确的表名"))
    val zkurl: String = parameters.getOrElse("zkurl",sys.error("请指定正确的zookeeper地址"))
    val familyStr: String = parameters.getOrElse("family",sys.error("请指定有效列族"))

    val connection:Connection=HBaseDSUtils.getConnection(zkurl)
    val admin: Admin = connection.getAdmin
    if(admin.tableExists(TableName.valueOf(tableName))){
      mode match {
        case SaveMode.Overwrite=>
          admin.disableTable(TableName.valueOf(tableName))
          admin.deleteTable(TableName.valueOf(tableName))
          HBaseDSUtils.createHBaseTable(zkurl,tableName,familyStr.split(","))
          relation.insert(data,true)
        case SaveMode.Append=>
          relation.insert(data,false)
        case SaveMode.ErrorIfExists=>sys.error(s"${tableName}已经存在了")
        case SaveMode.Ignore=>
      }
    }else{
      HBaseDSUtils.createHBaseTable(zkurl,tableName,familyStr.split(","))
      relation.insert(data,true)
    }
    relation
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val relation: HBaseRelation = HBaseRelation(parameters)(sqlContext)
    relation.buildScan()
    relation
  }
}
