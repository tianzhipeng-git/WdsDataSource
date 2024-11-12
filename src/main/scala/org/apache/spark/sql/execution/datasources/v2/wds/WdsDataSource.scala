package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.wds.WdsFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class WdsDataSource extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[WdsFileFormat];
  
  override def shortName(): String = "wds"
  
  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    WdsTable(tableName, sparkSession, optionsWithoutPaths, paths, None)
  }


  override protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    WdsTable(
      tableName, sparkSession, optionsWithoutPaths, paths, Some(schema))
  }
}
