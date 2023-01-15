import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}

object PopularTime {
  private def prepareStamp(stamp: String): LocalTime = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    LocalDateTime.parse(stamp, formatter).toLocalTime
  }

  def apply(taxiInfoDF: DataFrame, dataPath: String): RDD[(LocalTime, Int)] = {

    val popularTime =
      taxiInfoDF.rdd
        .map(trip => (prepareStamp(trip(1).toString), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)

    popularTime.take(20).foreach(println)

    popularTime.repartition(1)
      .map(row => row._1.toString + " " + row._2.toString)
      .saveAsTextFile(s"$dataPath/popular_time")

    popularTime
  }
}
