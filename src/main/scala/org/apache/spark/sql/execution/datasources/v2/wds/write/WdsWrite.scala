package org.apache.spark.sql.execution.datasources.v2.wds.write

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.internal.Logging
import java.nio.charset.{Charset, StandardCharsets}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.json.JacksonGenerator
import java.io.{ByteArrayOutputStream, OutputStreamWriter, OutputStream}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.v2.wds.WdsOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class WdsWrite(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo) extends FileWrite {

    override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
        val conf = job.getConfiguration
        val wdsOptions = new WdsOptions(
            CaseInsensitiveMap(options),
            sqlConf.sessionLocalTimeZone,
            sqlConf.columnNameOfCorruptRecord)
        wdsOptions.compressionCodec.foreach { codec =>
            CompressionCodecs.setCodecConfiguration(conf, codec)
        }
        new OutputWriterFactory {
            override def getFileExtension(context: TaskAttemptContext): String = {
                ".tar" + CodecStreams.getCompressionExtension(context)
            }

            override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
                new WdsOutputWriter(path, wdsOptions, dataSchema, context)
            }
        }
    }
}

class WdsOutputWriter(
    val path: String,
    options: WdsOptions,
    dataSchema: StructType,
    context: TaskAttemptContext) extends OutputWriter with Logging {

    private val encoding = options.encoding match {
        case Some(charsetName) => Charset.forName(charsetName)
        case None => StandardCharsets.UTF_8
    }
    
    private val tarStream: TarArchiveOutputStream = {
        val pth = new Path(path)
        val fs = pth.getFileSystem(context.getConfiguration)
        val outputStream: OutputStream = fs.create(new Path(path), false)
        
        val compressionCodecs = new CompressionCodecFactory(context.getConfiguration)
        val codecStream = Option(compressionCodecs.getCodec(pth))
            .map(codec => codec.createOutputStream(outputStream))
            .getOrElse(outputStream)

        new TarArchiveOutputStream(codecStream)
    }

    private val keyFieldIndex = dataSchema.getFieldIndex(options.keyField).get
    private val binaryFields = dataSchema.filter(_.dataType == BinaryType)

    private val jsonFields = dataSchema.filter(_.dataType != BinaryType)
    private val jsonRow = new GenericInternalRow(jsonFields.length)
    private val jsonStream = new ByteArrayOutputStream()
    private val gen = new JacksonGenerator(StructType(jsonFields), new OutputStreamWriter(jsonStream, encoding), options)

    override def write(row: InternalRow): Unit = {
        val key = row.get(keyFieldIndex, StringType)
        // 写二进制文件
        for (field <- binaryFields) {
            val index = dataSchema.getFieldIndex(field.name).get
            val valueBytes = row.get(index, field.dataType).asInstanceOf[Array[Byte]]
            
            val entry = new TarArchiveEntry(s"${key}.${field.name}")
            entry.setSize(valueBytes.length)
            tarStream.putArchiveEntry(entry)
            tarStream.write(valueBytes)
            tarStream.closeArchiveEntry()
        }
        
        // 写json文件
        println(s"jsonFields: ${jsonFields}")
        jsonFields.zipWithIndex.foreach { case (field, jsonFieldIdx) =>
            val srcIdx = dataSchema.getFieldIndex(field.name).get
            if (row.isNullAt(srcIdx)) {
              jsonRow.setNullAt(jsonFieldIdx)
            } else {
              jsonRow.update(jsonFieldIdx, row.get(srcIdx, field.dataType))
            }
        }
        println(s"jsonRow: ${jsonRow}")
        gen.write(jsonRow)
        gen.flush()

        val jsonBytes = jsonStream.toByteArray
        jsonStream.reset()
        val jsonEntry = new TarArchiveEntry(s"${key}.json")
        jsonEntry.setSize(jsonBytes.length)
        tarStream.putArchiveEntry(jsonEntry)
        tarStream.write(jsonBytes)
        tarStream.closeArchiveEntry()
    }

    override def close(): Unit = {
        tarStream.close()
        gen.close()
    }
}