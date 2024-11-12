package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSuite

import java.util.Collections
import java.io.File
import org.apache.spark.sql.execution.datasources.v2.wds.read.WdsScan
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JacksonParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.commons.codec.digest.DigestUtils

class WdsTableReadTest extends FunSuite {
  val spark = SparkSession.builder()
    .config("spark.sql.files.maxPartitionBytes", 1)
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val project_root_dir = new File(getClass.getResource("/").getPath).getParentFile.getParentFile.getParentFile
  val tarFile = s"$project_root_dir/src/test/resources/test.tar"

  ignore("test extra schema") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val hadoopConf = spark.sessionState.newHadoopConfWithOptions(CaseInsensitiveMap.apply(Map()))
    
    val stream = CodecStreams.createInputStreamWithCloseResource(hadoopConf, new Path(tarFile))
    // val fs = FileSystem.get(hadoopConf)
    // val stream = fs.open(new Path(tarFile))
    // Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => stream.close()))

    println(s"schema: ${Utils.inferSchema(stream, spark)}")

    val parsedOptions =
      new JSONOptionsInRead(CaseInsensitiveMap.apply(Map()), "UTC", "corrupt")
    val parser = new JacksonParser(
      schema,
      parsedOptions,
      allowArrayAsStructs = true,
      filters = Nil
    )

    // val factory = parsedOptions.buildJsonFactory()
    // val realJsonParser = factory.createParser(jsonString)

    // val jsonIterator = parser.parse(
    //   jsonString,
    //   (factory, fafa: String) => factory.createParser(fafa),
    //   (fafa: String) => UTF8String.fromString(fafa)
    // )

    // println(jsonIterator.iterator.next())
  }

  ignore("use underlying api to read tar") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("jpg", BinaryType)
    ))
    
    // 创建WdsTable实例
    val options = new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
    val wdsTable = WdsTable(
      name = "test_table",
      sparkSession = spark,
      options = options,
      paths = Seq(tarFile),
      userSpecifiedSchema = Some(schema)
    )

    // 调用newScanBuilder
    val scanBuilder = wdsTable.newScanBuilder(options)
    val scan: WdsScan = scanBuilder.build().asInstanceOf[WdsScan]
    println(scan.dataSchema)
    println(scan.readDataSchema)
    println(scan.readPartitionSchema)

    //real read file test
    val readerFactory = scan.createReaderFactory()
    val reader = readerFactory.buildReader(new PartitionedFile(InternalRow.empty, tarFile, 0, 0, Array.empty, 0, 0))
    // print all from reader
    while (reader.next()) {
      val internalRow = reader.get()
      val row = new GenericRowWithSchema(internalRow.toSeq(scan.readDataSchema).toArray, scan.readDataSchema)
      println(row)
      val jpg = row.getAs[Array[Byte]](2)
      //md5 jpg
      val md5 = DigestUtils.md5Hex(jpg)
      assert(md5 == "b019b5a751688233b0d355a818bcba1c")
    }

    
  }

  ignore("use datasource api to read tar") {
    val df = spark.read.format("org.apache.spark.sql.execution.datasources.v2.wds.WdsDataSource").load(tarFile)
    df.printSchema()
    df.show()
    print(df.count())
    print(df.rdd.partitions)
  }

  test("use datasource api to read tar v1") {
    val tarFile = "target/test1"
    val df = spark.read.format("wds").load(tarFile)
    df.printSchema()
    df.show()
    println(s"count: ${df.count()}")
    println(s"partitions: ${df.rdd.partitions.length}")
  }
} 