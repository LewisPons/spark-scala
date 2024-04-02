package org.lewispons.sparkcourse

import org.apache.spark.sql.{functions, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{
  avg,
  col,
  concat,
  current_timestamp,
  expr,
  lit,
  max,
  row_number,
  year
}
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark-video-tutorial")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    /* Different ways to call columns
      df.select("Date", "Open", "Close").show()
      val column = df("Date")
      col("Date")
      import spark.implicits._
      s"$Date"

      df.select(df("Date"), df("Open"), df("Close")).show()
      df.select(df("Date"), df("Open"), df("Close")).show()
     -------------------------- */

    /* BASIC Spark manipulation
      val column = df.apply("Open")
      val newColumn = (column + 2.0).as("OpenIncreaseBy2")
      val columnString = column.cast(StringType).as("OpenAsString")

      val litColumn = lit(2.0)
      val newColumnString = concat(columnString, lit("Hello World")).as("ssñdfr")

      df.select(column, newColumn, columnString, newColumnString)
        .show(truncate = false)
     -------------------------- */

    /* SQL EXPRESSIONS
    val timestampFromExpr = expr(
      "cast(current_timestamp() as string) as timestampExpre"
    ) // same approach but not reccomended
    val timestampFromFunc =
      current_timestamp().cast(StringType).as("timestampFunc")

    df.select(timestampFromExpr, timestampFromFunc).show()

    df.createTempView("df")
    spark.sql("SELECT * FROM df").show()
    -------------------------- */

    /*
        Exercise grouping
     */

    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    // df.columns.map(c => col(c).as(c.toLowerCase())) alternative
    val stockData = df.select(renameColumns: _*)
    // .withColumn("diff", col("close") - col("open"))
    // .filter(col("close") > col("open") * 1.1)

    stockData
      .groupBy(year($"date").as("year"))
      .agg(
        max($"close").as("maxClose"),
        avg($"close").as("avgClose")
      )
      .sort($"maxClose".desc)

    stockData
      .groupBy(year($"date").as("year"))
      .max("close", "high")

    /*
      Exercise grouping with window functions
     */

    val highestClosingPricesPerYearData = highestClosingPricesPerYear(stockData)

  }

  def highestClosingPricesPerYear(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    val windowPart =
      Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    df
      .withColumn("rank", row_number().over(windowPart))
      .filter($"rank" === 1)
      .drop("rank")
      .sort($"close".desc)
  }

}