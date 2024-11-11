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

import java.io.InputStream

class TarTest extends FunSuite {
  test("write tar") {
    val project_root_dir = new File(getClass.getResource("/").getPath).getParentFile.getParentFile.getParentFile
    println(s"project_root_dir: $project_root_dir")
    val tarFile = s"$project_root_dir/src/test/resources/test.tar"
    val tar = new TarArchiveOutputStream(new FileOutputStream(tarFile))

    val img = getClass.getResourceAsStream("/1.jpg")
    val imgBytes = IOUtils.toByteArray(img)
    
    for (i <- 0 until 10) {
      val json = Json.toJson(Map[String, JsValue](
        "name" -> JsString(s"name$i"),
        "age" -> JsNumber(i),
        "city" -> JsString("New York")
      ))
      val jsonBytes = json.toString.getBytes
      val jsonEntry = new TarArchiveEntry(s"$i.json")
      jsonEntry.setSize(jsonBytes.length)
      tar.putArchiveEntry(jsonEntry)
      tar.write(jsonBytes)
      tar.closeArchiveEntry()

      val imgEntry = new TarArchiveEntry(s"$i.jpg") 
      imgEntry.setSize(imgBytes.length)
      tar.putArchiveEntry(imgEntry)
      tar.write(imgBytes)
      tar.closeArchiveEntry()
    }
  }

  def typicalTarEntries(file_stream: InputStream): Map[String, TarArchiveEntry] = {
    val tar = new TarArchiveInputStream(file_stream)
    val firstEntry = tar.getNextEntry().asInstanceOf[TarArchiveEntry]
    val (firstBaseName, firstExtension) = Utils.splitFileName(firstEntry.getName)
    var entriesMap = Map[String, TarArchiveEntry]()
    entriesMap += (firstExtension -> firstEntry)

    Stream.continually(tar.getNextEntry())
      .takeWhile(_ != null)
      .map(entry => (entry.asInstanceOf[TarArchiveEntry], Utils.splitFileName(entry.getName)))
      .takeWhile { case (_, (baseName, _)) => baseName == firstBaseName }
      .foreach { case (entry, (_, extension)) =>
        entriesMap += (extension -> entry)
      }
    
    println(s"收集到的文件类型: ${entriesMap.keys.mkString(", ")}")
    entriesMap
  }

  test("read tar") {
    val project_root_dir = new File(getClass.getResource("/").getPath).getParentFile.getParentFile.getParentFile
    val tarFile = s"$project_root_dir/src/test/resources/test.tar"
    val entriesMap = Using(new FileInputStream(tarFile)) { stream =>
      typicalTarEntries(stream)
    }
    println(s"收集到的文件类型: ${entriesMap.keys.mkString(", ")}")
  }
}