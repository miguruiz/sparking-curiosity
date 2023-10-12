import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import utils.data._

object datasets extends App {

  implicit val spark = SparkSession.builder()
    .appName("FakeBookingsDataFrame")
    .master("local[*]")
    .config("spark.log.level", "ERROR")
    .config("spark.sql.adaptive.enabled","false")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("*** bookingFactDF *** ")
  bookingFactDF.show(100, false)

  println("*** clientDataDF *** ")
  clientDataDF.show(100, false)

  println("*** clientSurveyDataDF *** ")
  clientSurveyDataDF.show(100, false)

  println("*** countryInformationDf *** ")
  countryInformationDf.show(100, false)

  println("*** currencyExchangesDf *** ")
  currencyExchangesDf.show(100, false)
}
