package io.github.tianzhipeng_git.wds

import org.apache.spark.sql.SparkSession

// hello world
object HW {
  def add(a: Int, b:Int) = a + b

  def main(args: Array[String]): Unit = {
    println("Hello, World!")
    println(add(12,1))
  }
}
