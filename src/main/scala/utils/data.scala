package utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.Instant

import scala.util.Try

object data {

  def bookingFactDF(implicit spark: SparkSession) = createDf(
    Seq(
      ("B001", "C001", Timestamp.from(Instant.parse("2023-09-14T17:20:16.468Z")), 100.0,"USD"),
      ("B002", "C002", Timestamp.from(Instant.parse("2023-09-15T17:20:16.468Z")), 200.0,"USD"),
      ("B003", "C003", Timestamp.from(Instant.parse("2023-09-16T17:20:16.468Z")), 150.0,"USD"),
      ("B004", "C004", Timestamp.from(Instant.parse("2023-09-14T17:20:16.468Z")), 100.0,"USD"),
      ("B005", "C005", Timestamp.from(Instant.parse("2023-09-15T17:20:16.468Z")), 200.0,"USD"),
      ("B006", "C005", Timestamp.from(Instant.parse("2023-09-16T17:20:16.468Z")), 150.0,"USD"),
      ("B007", "C001", Timestamp.from(Instant.parse("2023-09-14T17:20:16.468Z")), 100.0,"USD"),
      ("B008", "C002", Timestamp.from(Instant.parse("2023-09-15T17:20:16.468Z")), 200.0,"USD"),
      ("B009", "C003", Timestamp.from(Instant.parse("2023-09-16T17:20:16.468Z")), 150.0,"USD"),
      ("B010", "C001", Timestamp.from(Instant.parse("2023-09-14T17:20:16.468Z")), 100.0,"USD"),
      ("B011", "C002", Timestamp.from(Instant.parse("2023-09-15T17:20:16.468Z")), 200.0,"EUR"),
      ("B012", "C003", Timestamp.from(Instant.parse("2023-09-16T17:20:16.468Z")), 150.0,"GBP"),
    )
    ,
    StructType(Seq(
      StructField("booking_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("booking_date_time", TimestampType, nullable = false),
      StructField("total_amount", DoubleType, nullable = false),
      StructField("currency", StringType, nullable = false)
    ))
  )


  def clientDataDF(implicit spark: SparkSession) = createDf(
    Seq(
      ("C001", "NLD",18),
      ("C002", "ESP",19),
      ("C003", "TUR",20),
      ("C004", "CHN",21),
      ("C005", "AUS",22),
    )
    ,
    StructType(Seq(
      StructField("customer_id", StringType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
    ))
  )

  def countryInformationDf(implicit spark: SparkSession) = createDf(
    Seq(
      ("NLD", 17.53),
      ("ESP", 47.42),
      ("TUR", 84.78),
      ("CHN", 1425.533),
      ("AUS", 25.69),
    )
    ,
    StructType(Seq(
      StructField("country_id", StringType, nullable = false),
      StructField("population", DoubleType, nullable = false),
    ))
  )

  def clientSurveyDataDF(implicit spark: SparkSession) = createDf(
    Seq(
      ("C001", "red",  "chicken",7),
      ("C002", "blue","pig",8),
      ("C003", "green", "carrot",9),
      ("C004", "yellow", "cat", 7),
      ("C005", "pink", "shark",5 ),
    )
    ,
    StructType(Seq(
      StructField("customer_id", StringType, nullable = false),
      StructField("fav_color", StringType, nullable = false),
      StructField("fav_animal", StringType, nullable = false),
      StructField("nps_score", IntegerType, nullable = false),
    ))
  )


  def currencyExchangesDf(implicit spark: SparkSession) = createDf(
    Seq(
      ("EUR", 1.05,1),
      ("GBP", 1.22,1),
    )
    ,
    StructType(Seq(
      StructField("currency_id", StringType, nullable = false),
      StructField("exchange_rate_usd", DoubleType, nullable = false),
    ))
  )




  def createDf[T](rows: Seq[T], schema: Seq[StructField])(implicit spark: SparkSession): DataFrame = {
    val data = Try(rows.map(row => Row(row.asInstanceOf[Product].productIterator.toSeq: _*)))
      .getOrElse(rows.map(Row(_)))
    spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema))
  }

}


