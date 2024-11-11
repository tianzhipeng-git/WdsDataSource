package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}

import java.io._

object Using {
  def apply[A <: Closeable, B](resource: A)(block: A => B): B = {
    try {
      block(resource)
    } finally {
      resource.close()
    }
  }
}

object Utils {

  def splitFileName(name: String): (String, String) = {
    val lastDotIndex = name.lastIndexOf(".")
    if (lastDotIndex >= 0) {
      (name.substring(0, lastDotIndex), name.substring(lastDotIndex + 1))
    } else {
      (name, "")
    }
  }
  
  /**
   * 收集同一个basename的所有entry;
   * 1. 不处理currentEntry
   * 2. 执行完后, tar.currentEntry指向了下一组entry
   */
  def collectEntrySameName(tar: TarArchiveInputStream, sameBaseName: String): Map[String, Array[Byte]] = {
      Stream.continually(tar.getNextEntry())
        .takeWhile(_ != null)
        .map(entry => (entry.asInstanceOf[TarArchiveEntry], Utils.splitFileName(entry.getName)))
        .takeWhile { case (_, (baseName, _)) => baseName == sameBaseName }
        .map { case (entry, (_, extension)) =>
          extension -> IOUtils.toByteArray(tar)
        }
        .toMap
  }

  def inferSchema(file_stream: InputStream, spark: SparkSession): StructType = {
    val tar = if (file_stream.isInstanceOf[TarArchiveInputStream]) {
      file_stream.asInstanceOf[TarArchiveInputStream]
    } else {
      new TarArchiveInputStream(file_stream)
    }
    
    val firstEntry = tar.getNextEntry().asInstanceOf[TarArchiveEntry]
    val (firstBaseName, firstExtension) = Utils.splitFileName(firstEntry.getName)
    
    var entriesMap = Map[String, Array[Byte]]()
    entriesMap += (firstExtension -> IOUtils.toByteArray(tar))
    entriesMap ++= collectEntrySameName(tar, firstBaseName)

    var schema = new StructType()
    for ((extension, entry) <- entriesMap) {
      if (extension == "json") {
        val jsonString = new String(entry)
        val df = spark.read.json(spark.createDataset(Seq(jsonString))(Encoders.STRING))
        val jsonSchema = df.schema
        jsonSchema.fields.map(field => schema = schema.add(field))
      } else {
        schema = schema.add(StructField(extension, BinaryType, nullable = true))
      }
    }
    schema
  }
}
