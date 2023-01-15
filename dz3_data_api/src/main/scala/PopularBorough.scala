import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object PopularBorough {
  def apply(taxiInfoDF: DataFrame, taxiDictDF: DataFrame, dataPath: String): Dataset[Row] = {

    val popularBorough = taxiInfoDF.select("DOLocationID")
      .join(taxiDictDF, taxiInfoDF.col("DOLocationID") === taxiDictDF.col("LocationID"))
      .groupBy(col("Borough"))
      .agg(count(col("Borough")) as "count")
      .sort(col("count").desc)

    popularBorough
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(s"$dataPath/popular_borough")

    popularBorough
  }
}
