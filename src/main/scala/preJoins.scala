import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.spark_partition_id
import utils.data._

object preJoins extends App {

  implicit val spark = SparkSession.builder()
    .appName("FakeBookingsDataFrame")
    .master("local[*]")
    .config("spark.log.level", "ERROR")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .getOrCreate()


  bookingFactDF.createOrReplaceTempView("bookingFactDf")
  currencyExchangesDf.createOrReplaceTempView("currencyExchangesDf")
  clientSurveyDataDF.createOrReplaceTempView("clientSurveyDataDf")
  clientDataDF.createOrReplaceTempView("clientDataDF")
  countryInformationDf.createOrReplaceTempView("countryInformationDf")


  println("BEFORE DOING ANYTHING")

  val normalQuery = spark.sql(
    """
    SELECT * FROM bookingFactDF bookings

    LEFT JOIN clientDataDF  client
    ON  bookings.customer_id = client.customer_id

    LEFT JOIN countryInformationDf  country
    ON client.country = country.country_id
    """)

  normalQuery.explain(mode = "formatted")
  normalQuery
    .show(100, false)

  println("COMMON TABLE EXPRESSION")
  val commontableExpressionDf = spark.sql(
    """
      WITH tt AS (
      SELECT * FROM clientDataDF client
      LEFT JOIN countryInformationDf country
      ON client.country = country.country_id
    )

    SELECT * FROM bookingFactDF bookings
    LEFT JOIN tt
    ON  bookings.customer_id = tt.customer_id

    """)

   commontableExpressionDf.explain(mode = "formatted")


  normalQuery
    .show(100, false)

  commontableExpressionDf.show(100, false)


}
