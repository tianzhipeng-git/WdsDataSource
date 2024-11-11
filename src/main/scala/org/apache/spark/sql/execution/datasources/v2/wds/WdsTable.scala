package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.wds.write.WdsWrite

case class WdsTable(
                     name: String,
                     sparkSession: SparkSession,
                     options: CaseInsensitiveStringMap,
                     paths: Seq[String],
                     userSpecifiedSchema: Option[StructType]
                   ) extends FileTable(sparkSession, options, paths, userSpecifiedSchema)
  with SupportsWrite
  with SupportsRead {

  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  override def formatName: String = "wds"

  //dataSchema属性, 来自FileTable类, 是用户指定schema排除partitionSchema, 或者inferSchema
  //schema属性, 来自FileTable类,是dataSchema和partitionSchema的并集
  override def inferSchema(
                            files: Seq[FileStatus]
                          ): Option[StructType] = {
    val file: FileStatus = files.find(f => f.getPath.toString.endsWith(".tar") || f.getPath.toString.endsWith(".tar.gz"))
      .getOrElse(throw new IllegalArgumentException("没有找到.tar或.tar.gz文件"))

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val fs = file.getPath().getFileSystem(hadoopConf)
    Using(fs.open(file.getPath)) { stream =>
      val schema = Utils.inferSchema(stream, sparkSession)
      Some(schema)
    }
  }

  override def newScanBuilder(
                               options: CaseInsensitiveStringMap
                             ): ScanBuilder = {
    new read.WdsScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder {
      override def build(): Write = WdsWrite(paths, formatName, supportsDataType, info)
    }
  }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }
}
