import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, spark_partition_id}
import utils.data._

object skewJoins extends App {

  implicit val spark = SparkSession.builder()
    .appName("FakeBookingsDataFrame")
    .master("local[*]")
    .config("spark.log.level", "ERROR")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .getOrCreate()


  bookingFactDF.createOrReplaceTempView("bookingFactDF")
  currencyExchangesDf.createOrReplaceTempView("currencyExchangesDf")


  println("BEFORE DOING ANYTHING")

  val untreatedSkew = spark.sql(
    """
      SELECT * FROM bookingFactDF
      LEFT JOIN currencyExchangesDf
      ON
      bookingFactDF.currency = currencyExchangesDf.currency_id
    """)
    .withColumn("partition", spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")

  untreatedSkew
    .show(100, false)

  println("AFTER DOING TREATING THE SKEW")
  val treatingSkew = spark.sql(
    """
      SELECT * FROM bookingFactDF
      LEFT JOIN currencyExchangesDf
      ON
      coalesce(nullif(trim(bookingFactDF.currency),'USD'),  bookingFactDF.customer_id) = currencyExchangesDf.currency_id
    """)
  val treatingSkewPartitions = treatingSkew
    .withColumn("partition", spark_partition_id())
      .groupBy("partition")
      .count()
      .orderBy("partition")

  treatingSkewPartitions
      .show(100, false)


  println("BREAKING THE JOIN")
  val breakingTheJoin = spark.sql(
    """
      SELECT bf.*, null as exchange_rate_usd, null as currency_id FROM bookingFactDF bf where currency = 'USD'
    UNION
    (SELECT * FROM bookingFactDF
    LEFT JOIN currencyExchangesDf
    ON bookingFactDF.currency = currencyExchangesDf.currency_id
    WHERE currency != 'USD')
    """)

  treatingSkew.explain(mode = "formatted")
  breakingTheJoin.explain(mode = "formatted")

}
