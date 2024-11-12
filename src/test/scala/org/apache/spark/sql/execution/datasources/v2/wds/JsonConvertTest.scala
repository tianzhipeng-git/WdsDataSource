package org.apache.spark.sql.execution.datasources.v2.wds

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptionsInRead, JacksonParser}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class JsonConvertTest extends AnyFunSuite {
  ignore("test") {
    val jsonString = "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}"
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("city", StringType)
      )
    )
    println(jsonString)

    val parsedOptions =
      new JSONOptionsInRead(CaseInsensitiveMap.apply(Map()), "UTC", "corrupt")
    val parser = new JacksonParser(
      schema,
      parsedOptions,
      allowArrayAsStructs = true,
      filters = Nil
    )
    val fieldConverters =
      schema.map(_.dataType).map(parser.makeConverter).toArray

    val factory = parsedOptions.buildJsonFactory()
    val realJsonParser = factory.createParser(jsonString)

    // realJsonParser to map
    // val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    // val jsonMap = mapper.readValue(realJsonParser, classOf[java.util.Map[String, Object]])
    // println(jsonMap)
    print(realJsonParser.getCurrentName())

    val row = new GenericInternalRow(schema.length)
    println(fieldConverters.mkString(","))
    fieldConverters.zipWithIndex.foreach { case (converter, index) =>
      row.update(index, converter(realJsonParser))
    }
    println(row)
  }

  ignore("test iter json") {
    val jsonString = "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}"
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("city", StringType)
      )
    )
    println(jsonString)

    val parsedOptions =
      new JSONOptionsInRead(CaseInsensitiveMap.apply(Map()), "UTC", "corrupt")
    val parser = new JacksonParser(
      schema,
      parsedOptions,
      allowArrayAsStructs = true,
      filters = Nil
    )
    val fieldConverters =
      schema.map(_.dataType).map(parser.makeConverter).toArray

    val factory = parsedOptions.buildJsonFactory()
    val realJsonParser = factory.createParser(jsonString)

    val jsonIterator = parser.parse(
      jsonString,
      (factory, fafa: String) => factory.createParser(fafa),
      (fafa: String) => UTF8String.fromString(fafa)
    )

    println(jsonIterator.iterator.next())
  }

  test("test extra schema") {
    val jsonString = "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}"
    val schema = StructType(
      Seq(
        // StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("city", StringType),
        StructField("jpg", BinaryType)
      )
    )
    println(jsonString)

    val parsedOptions =
      new JSONOptionsInRead(CaseInsensitiveMap.apply(Map()), "UTC", "corrupt")
    val parser = new JacksonParser(
      schema,
      parsedOptions,
      allowArrayAsStructs = true,
      filters = Nil
    )

    val factory = parsedOptions.buildJsonFactory()
    val realJsonParser = factory.createParser(jsonString)

    val jsonIterator = parser.parse(
      jsonString,
      (factory, fafa: String) => factory.createParser(fafa),
      (fafa: String) => UTF8String.fromString(fafa)
    )

    println(jsonIterator.iterator.next())
  }
}
