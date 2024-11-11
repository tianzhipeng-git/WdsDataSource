package org.apache.spark.sql.execution.datasources.v2.wds.read

import org.apache.spark.sql.catalyst.StructFilters
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead

//TODO: 特性支持: filter pushdown, columnPruning/schema问题/partition, split, corrupt, 自定义的options
//copy from CSVScan/JsonScan
case class WdsScan (
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends FileScan {

  override def isSplitable(path: Path): Boolean = {
    false 
  }

  override def getFileUnSplittableReason(path: Path): String = {
    "xxx"
  }

  override def createReaderFactory(): FilePartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val parsedOptions = new JSONOptionsInRead(
                          options.asScala.toMap,
                          sparkSession.sessionState.conf.sessionLocalTimeZone,
                          sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    WdsPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, pushedFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case c: WdsScan => super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      equivalentFilters(pushedFilters, c.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters))
  }
}

case class WdsScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): WdsScan = {
    WdsScan(
      sparkSession,
      fileIndex,
      dataSchema, //来自WdsTable
      readDataSchema(), //来自FileScanBuilder中, 需要读的数据字段, 不含分区字段
      readPartitionSchema(), //来自FileScanBuilder中, 分区字段
      options,
      pushedDataFilters,
      partitionFilters,
      dataFilters)
  }

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    if (sparkSession.sessionState.conf.csvFilterPushDown) {
      StructFilters.pushedFilters(dataFilters, dataSchema)
    } else {
      Array.empty[Filter]
    }
  }
}