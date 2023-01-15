import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

import java.util.Properties

object DistanceDistribution {
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "otus"
  val password = "otus"
  val table = "distance_distribution"

  val connectionProperties = new Properties()
  connectionProperties.put("user", user)
  connectionProperties.put("password", password)
  connectionProperties.put("driver", driver)

  def apply(taxiInfoDF: DataFrame, taxiDictDF: DataFrame): Dataset[Row] = {

    val distanceDistribution = taxiInfoDF.select("DOLocationID", "trip_distance")
      .join(taxiDictDF, taxiInfoDF.col("DOLocationID") === taxiDictDF.col("LocationID"))
      .filter(col("trip_distance") > 0)
      .groupBy(col("Borough") as "borough")
      .agg(
        count(col("Borough")) as "count",
        stddev(col("trip_distance")) as "std_distance",
        mean(col("trip_distance")) as "mean_distance",
        max(col("trip_distance")) as "max_distance",
        min(col("trip_distance")) as "min_distance",
      )
      .sort(col("count").desc)

    distanceDistribution
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "distance_distribution", connectionProperties)

    distanceDistribution
  }
}
