package org.apache.spark.sql.execution.datasources.wds

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{
  FileFormat,
  OutputWriter,
  OutputWriterFactory
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources.{
  CodecStreams,
  OutputWriter,
  OutputWriterFactory
}
import org.apache.spark.sql.catalyst.json.{JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.v2.wds.{Utils, WdsOptions}
import org.apache.spark.sql.execution.datasources.v2.wds.write.WdsOutputWriter
import org.apache.spark.sql.execution.datasources.v2.wds.read.WdsPartitionReaderFactory
import org.apache.spark.util.SerializableConfiguration

class WdsFileFormat
    extends FileFormat
    with DataSourceRegister
    with Logging
    with Serializable {

  override def shortName(): String = "wds"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]
  ): Option[StructType] = {
    Utils.inferSchema(sparkSession, options, files)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    val conf = job.getConfiguration
    val sqlConf = sparkSession.sessionState.conf
    val wdsOptions = new WdsOptions(
      CaseInsensitiveMap(options),
      sqlConf.sessionLocalTimeZone,
      sqlConf.columnNameOfCorruptRecord
    )
    wdsOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext
      ): OutputWriter = {
        new WdsOutputWriter(path, wdsOptions, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".tar" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {
    // 在这里实现读取逻辑
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord
    )
    val readerFactory = new WdsPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      parsedOptions,
      filters
    )
    readerFactory.buildIterator
  }
}
