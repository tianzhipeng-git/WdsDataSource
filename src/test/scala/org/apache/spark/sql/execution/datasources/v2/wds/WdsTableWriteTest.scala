package org.apache.spark.sql.execution.datasources.v2.wds

import org.scalatest.FunSuite
import org.apache.spark.sql.execution.datasources.v2.wds.write.WdsOutputWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import java.io.File
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.IOUtils
import java.io.FileInputStream
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.SparkSession

import scala.io.Source
import org.apache.spark.unsafe.types.UTF8String

class WdsOutputWriterTest extends FunSuite {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._
  
  ignore("use underlying api to write tar") {
    // 准备测试数据
    val schema = StructType(Seq(
      StructField("key", StringType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("photo", BinaryType)
    ))
    
    val options = new JSONOptions(
      Map("wds_keyfield" -> "key"),
      "UTC",
      "_corrupt_record"
    )

    
    val conf = new Configuration()
    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val outputPath = "target/" + context.getTaskAttemptID.toString + ".tar"
    //rm if exists
    new File(outputPath).delete()
    
    // 创建writer并写入数据
    val writer = new WdsOutputWriter(outputPath, options, schema, context)
    
    val photoBytes = "test photo data".getBytes()
    val row = InternalRow(
      UTF8String.fromString("user1"),     // key
      UTF8String.fromString("Alice"),     // name 
      25,                                 // age
      photoBytes                          // photo
    )
    
    writer.write(row)
    writer.close()
    
    // 验证写入的tar文件内容
    val tarIn = new TarArchiveInputStream(new FileInputStream(outputPath))
    
    // 读取并验证photo文件
    var entry = tarIn.getNextTarEntry
    assert(entry.getName == "user1.photo")
    val actualPhotoBytes = new Array[Byte](entry.getSize.toInt)
    IOUtils.readFully(tarIn, actualPhotoBytes)
    assert(actualPhotoBytes.sameElements(photoBytes))
    
    // 读取并验证json文件
    entry = tarIn.getNextTarEntry
    assert(entry.getName == "user1.json")
    val jsonContent = new String(IOUtils.toByteArray(tarIn))
    val json = parse(jsonContent)
    
    val expectedJson = parse("""
      {
        "key": "user1",
        "name": "Alice",
        "age": 25
      }
    """)
    
    assert(json == expectedJson)
    
    tarIn.close()
    // new File(outputPath).delete()
  }

  test("use datasource api to write tar") {
    //generate df 100 rows
    val img = getClass.getResourceAsStream("/1.jpg")
    val imgBytes = IOUtils.toByteArray(img)
    val df = spark.range(100).map(i => {
      (i.toString, s"name$i", i, imgBytes)
    }).toDF("key", "name", "age", "photo")

    df.show()

    df.write.format("org.apache.spark.sql.execution.datasources.v2.wds.WdsDataSource")
      .option("wds_keyfield", "key")
      .save("target/test1")
  }
} 