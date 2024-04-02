package org.lewispons.sparkcourse

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{
  DoubleType,
  StructField,
  StructType,
  DateType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class firstTest extends AnyFunSuite {

  private val spark = SparkSession
    .builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(
    Seq(
      StructField(name = "date", dataType = DateType, nullable = true),
      StructField(name = "open", dataType = DoubleType, nullable = true),
      StructField(name = "close", dataType = DoubleType, nullable = true)
    )
  )

  test("returns Highest Closing Prices For Year") {

    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    implicit val encoder = Encoders.row(schema)
    val testData = spark.createDataset(testRows)

    val actualRows = Main
      .highestClosingPricesPerYear(testData)
      .collect()

    actualRows should contain theSameElementsAs expected

  }

}