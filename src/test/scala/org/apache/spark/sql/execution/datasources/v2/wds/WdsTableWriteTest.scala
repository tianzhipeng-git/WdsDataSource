package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.v2.wds.write.WdsOutputWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileInputStream}

class WdsOutputWriterTest extends AnyFunSuite {
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
    
    val options = new WdsOptions(
      CaseInsensitiveMap(Map("wds_keyfield" -> "key")),
      "UTC",
      "_corrupt_record"
    )

    
    val conf = new Configuration()
    val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
    val outputPath = "target/" + context.getTaskAttemptID.toString + ".tar"
    //rm if exists
    FileUtils.deleteDirectory(new File(outputPath))
    
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
    val outputPath = "target/test1"
    FileUtils.deleteDirectory(new File(outputPath))
    //generate df 100 rows
    val img = getClass.getResourceAsStream("/1.jpg")
    val imgBytes = IOUtils.toByteArray(img)
    val df = spark.range(100).map(i => {
      (i.toString, s"name$i", i, imgBytes)
    }).toDF("key", "name", "age", "jpg")

    df.show()

    df.repartition(5).write.format("wds")
      .option("wds_keyfield", "key")
      .save(outputPath)
  }
} 