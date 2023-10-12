import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import utils.data._

object orderOfJoins extends App {

  implicit val spark = SparkSession.builder()
    .appName("FakeBookingsDataFrame")
    .master("local[*]")
    .config("spark.log.level", "ERROR")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    .getOrCreate()



  bookingFactDF.createOrReplaceTempView("bookingFactDf")
  currencyExchangesDf.createOrReplaceTempView("currencyExchangesDf")
  clientSurveyDataDF.createOrReplaceTempView("clientSurveyDataDf")
  clientDataDF.createOrReplaceTempView("clientDataDF")

  println("\n\n#### smallUnorderedJoins")


  val smallUnorderedJoins = spark.sql(
    """
       SELECT * FROM bookingFactDf bf
       LEFT JOIN clientDataDF cd1 ON bf.customer_id = cd1.customer_id
       LEFT JOIN currencyExchangesDf ce1 ON bf.currency = ce1.currency_id
       LEFT JOIN clientSurveyDataDf cs1 ON  bf.customer_id = cs1.customer_id
    """)

  smallUnorderedJoins.explain(mode="formatted" )

  println("\n\n#### smallOrderedJoins")

  val smallSortedJoins = spark.sql(
    """
       SELECT * FROM bookingFactDf bf
       LEFT JOIN clientDataDF cd1 ON bf.customer_id = cd1.customer_id
       LEFT JOIN clientSurveyDataDf cs1 ON  bf.customer_id = cs1.customer_id
       LEFT JOIN currencyExchangesDf ce1 ON bf.currency = ce1.currency_id
    """)

  smallSortedJoins.explain(mode="formatted" )


  /**
   * LETS SCALE THINGS...
   */

//  println("\n\n#### largeUnorderedJoins")
//  val largeUnorderedJoins = spark.sql(
//    """
//       SELECT * FROM bookingFactDf bf
//       LEFT JOIN clientDataDF cd1 ON bf.customer_id = cd1.customer_id
//       LEFT JOIN currencyExchangesDf ce1 ON bf.currency = ce1.currency_id
//       LEFT JOIN clientSurveyDataDf cs1 ON  bf.customer_id = cs1.customer_id
//
//       LEFT JOIN clientDataDF cd2 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN currencyExchangesDf ce2 ON bf.currency = ce2.currency_id
//       LEFT JOIN clientSurveyDataDf cs2 ON  bf.customer_id = cs2.customer_id
//
//       LEFT JOIN clientDataDF cd3 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN currencyExchangesDf ce3 ON bf.currency = ce2.currency_id
//       LEFT JOIN clientSurveyDataDf cs3 ON  bf.customer_id = cs2.customer_id
//
//       LEFT JOIN clientDataDF cd4 ON bf.customer_id = cd4.customer_id
//       LEFT JOIN currencyExchangesDf ce4 ON bf.currency = ce4.currency_id
//       LEFT JOIN clientSurveyDataDf cs4 ON  bf.customer_id = cs4.customer_id
//
//       LEFT JOIN clientDataDF cd5 ON bf.customer_id = cd4.customer_id
//       LEFT JOIN currencyExchangesDf ce5 ON bf.currency = ce4.currency_id
//       LEFT JOIN clientSurveyDataDf cs5 ON  bf.customer_id = cs4.customer_id
//    """)
//
//  largeUnorderedJoins.explain(mode="formatted" )
//
//  println("\n\n#### largeMaybeOrderedJoins")
//  val largeMaybeOrderedJoins = spark.sql(
//    """
//       SELECT * FROM bookingFactDf bf
//       LEFT JOIN clientDataDF cd1 ON bf.customer_id = cd1.customer_id
//       LEFT JOIN clientSurveyDataDf cs1 ON  bf.customer_id = cs1.customer_id
//
//       LEFT JOIN clientDataDF cd2 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN clientSurveyDataDf cs2 ON  bf.customer_id = cs2.customer_id
//
//       LEFT JOIN clientDataDF cd3 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN clientSurveyDataDf cs3 ON  bf.customer_id = cs2.customer_id
//
//       LEFT JOIN clientDataDF cd4 ON bf.customer_id = cd4.customer_id
//       LEFT JOIN clientSurveyDataDf cs4 ON  bf.customer_id = cs4.customer_id
//
//       LEFT JOIN clientDataDF cd5 ON bf.customer_id = cd4.customer_id
//       LEFT JOIN clientSurveyDataDf cs5 ON  bf.customer_id = cs4.customer_id
//
//       LEFT JOIN currencyExchangesDf ce1 ON bf.currency = ce1.currency_id
//       LEFT JOIN currencyExchangesDf ce2 ON bf.currency = ce2.currency_id
//       LEFT JOIN currencyExchangesDf ce3 ON bf.currency = ce2.currency_id
//       LEFT JOIN currencyExchangesDf ce4 ON bf.currency = ce4.currency_id
//       LEFT JOIN currencyExchangesDf ce5 ON bf.currency = ce4.currency_id
//    """)
//
//  largeMaybeOrderedJoins.explain(mode = "formatted")
//
//  println("\n\n#### largeSortedJoins")
//  val largeSortedJoins = spark.sql(
//    """
//       SELECT * FROM bookingFactDf bf
//       LEFT JOIN clientDataDF cd1 ON bf.customer_id = cd1.customer_id
//       LEFT JOIN clientDataDF cd2 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN clientDataDF cd3 ON bf.customer_id = cd2.customer_id
//       LEFT JOIN clientDataDF cd4 ON bf.customer_id = cd4.customer_id
//       LEFT JOIN clientDataDF cd5 ON bf.customer_id = cd4.customer_id
//
//       LEFT JOIN clientSurveyDataDf cs1 ON  bf.customer_id = cs1.customer_id
//       LEFT JOIN clientSurveyDataDf cs2 ON  bf.customer_id = cs2.customer_id
//       LEFT JOIN clientSurveyDataDf cs3 ON  bf.customer_id = cs2.customer_id
//       LEFT JOIN clientSurveyDataDf cs4 ON  bf.customer_id = cs4.customer_id
//       LEFT JOIN clientSurveyDataDf cs5 ON  bf.customer_id = cs4.customer_id
//
//       LEFT JOIN currencyExchangesDf ce1 ON bf.currency = ce1.currency_id
//       LEFT JOIN currencyExchangesDf ce2 ON bf.currency = ce2.currency_id
//       LEFT JOIN currencyExchangesDf ce3 ON bf.currency = ce2.currency_id
//       LEFT JOIN currencyExchangesDf ce4 ON bf.currency = ce4.currency_id
//       LEFT JOIN currencyExchangesDf ce5 ON bf.currency = ce4.currency_id
//    """)
//
//  largeSortedJoins.explain(mode = "formatted")
}
