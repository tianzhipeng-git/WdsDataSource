package org.apache.spark.sql.execution.datasources.v2.wds
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream}
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import play.api.libs.json._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

import java.io.FileInputStream
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser, JsonInferSchema}
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import com.fasterxml.jackson.core._
import org.apache.spark.sql.Encoders

class InferSchemaTest extends FunSuite {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  test("read tar") {
    val tarFile = "target/test.tar"
    //check file exists
    assert(new File(tarFile).exists(), s"文件 $tarFile 不存在")
    val tar = new FileInputStream(tarFile)
    val schema = Utils.inferSchema(tar, spark)
    println(s"inferred schema: ${schema.json}")
  }
}
