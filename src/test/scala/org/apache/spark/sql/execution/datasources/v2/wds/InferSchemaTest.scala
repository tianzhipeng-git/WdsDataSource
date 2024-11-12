package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileInputStream}

class InferSchemaTest extends AnyFunSuite {
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  val sc = spark.sparkContext

  test("read tar") {
    val project_root_dir = new File(getClass.getResource("/").getPath).getParentFile.getParentFile.getParentFile
    println(s"project_root_dir: $project_root_dir")
    val tarFile = s"$project_root_dir/src/test/resources/test.tar"
    //check file exists
    assert(new File(tarFile).exists(), s"文件 $tarFile 不存在")
    val tar = new FileInputStream(tarFile)
    val schema = Utils.inferSchema(tar, spark)
    println(s"inferred schema: ${schema.json}")
  }
}
