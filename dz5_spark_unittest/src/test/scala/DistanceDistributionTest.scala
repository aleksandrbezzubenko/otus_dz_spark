import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DistanceDistributionTest extends SharedSparkSession {
  System.setProperty("hadoop.home.dir", "/")

  val dataPath = "src/main/resources/data"

  test("Distance Distribution Test") {
    val taxiInfoDF = spark.read.parquet(s"$dataPath/new_york_taxi_data")
    val taxiDictDF = spark.read
      .option("header", "true")
      .csv(s"$dataPath/taxi_zones.csv")

    val distanceDistribution = DistanceDistribution(taxiInfoDF, taxiDictDF)

    checkAnswer(
      distanceDistribution,
        Row("Manhattan", 295642,2.6319377112494804,2.1884251899256144,37.92,0.01)  ::
        Row("Queens", 13394,5.420778528649564,8.98944004778256,51.6,0.01) ::
        Row("Brooklyn", 12587,4.754780022484276,6.932374672280937,44.8,0.01) ::
        Row("Unknown", 6285,5.715976934424563,3.68733333333335,66.0,0.01) ::
        Row("Bronx", 1562,5.330517246526124,9.209398207426394,31.18,0.02) ::
        Row("EWR", 491,3.761422354588497,17.559816700610995,45.98,0.01) ::
        Row("Staten Island", 62,6.892080858225576,20.114032258064512,33.78,0.3) :: Nil
    )
  }
}