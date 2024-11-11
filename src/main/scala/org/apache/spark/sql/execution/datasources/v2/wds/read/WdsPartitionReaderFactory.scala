package org.apache.spark.sql.execution.datasources.v2.wds.read
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2._

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.execution.datasources.v2.wds.{Using, Utils}
import org.apache.spark.sql.catalyst.json.JacksonParser
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.json.CreateJacksonParser
import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.json.JSONOptions

case class WdsPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    parsedOptions: JSONOptions,
    filters: Seq[Filter]) extends FilePartitionReaderFactory {

    override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
        val conf = broadcastedConf.value.value
        val stream = CodecStreams.createInputStreamWithCloseResource(conf, new Path(file.filePath))

        val iter = new Iterator[InternalRow] {
            val parser = new JacksonParser(readDataSchema, parsedOptions, allowArrayAsStructs = true, filters)

            val row = new GenericInternalRow(readDataSchema.length)
            val tar = new TarArchiveInputStream(stream)
            var nextEntry = tar.getNextEntry()
            
            override def hasNext: Boolean = nextEntry != null

            override def next(): InternalRow = {
                if (nextEntry == null) {
                    throw new NoSuchElementException("No more elements")
                }
                //读取同一个basename的所有entry. TODO 支持列裁剪(跳过列)
                var currentEntries = Map[String, Array[Byte]]()
                val currentEntry = tar.getCurrentEntry()
                val (currentBaseName, extension) = Utils.splitFileName(currentEntry.getName)
                currentEntries += (extension -> IOUtils.toByteArray(tar))
                currentEntries ++= Utils.collectEntrySameName(tar, currentBaseName)
                nextEntry = tar.getCurrentEntry()
                
                //构造row
                for ((extension, entry) <- currentEntries) {
                    if (extension == "json") {
                        val jsonString = new String(entry)
                        val jsonIterator = parser.parse(jsonString, (factory, fafa:String) => factory.createParser(fafa), 
                            (fafa: String) => UTF8String.fromString(fafa))
                        val jsonRow = jsonIterator.iterator.next()
                        for (i <- 0 until readDataSchema.length) {
                            if (!jsonRow.isNullAt(i)) {
                                row.update(i, jsonRow.get(i, readDataSchema.fields(i).dataType))
                            }
                        }
                    } else {
                        val index = readDataSchema.getFieldIndex(extension)
                        if (index.isDefined) {
                            row.update(index.get, entry)
                        }
                    }
                }
                row
            }
        }
        val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
        new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, file.partitionValues)
        
    }
}