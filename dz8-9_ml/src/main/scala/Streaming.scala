import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.util.Properties

object Streaming extends App {

  val iris_labels          = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")
  val prediction_value_udf = udf((col: Double) => iris_labels(col))

  val sparkConf = new SparkConf()
    .setAppName("streaming")
    .setMaster("local[2]")
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))
  val sparkContext     = streamingContext.sparkContext

  val model = PipelineModel.load("src/main/resources/model")

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val kafkaSink = sparkContext.broadcast(KafkaSink(props))

  val kafkaParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> "localhost:29092",
    ConsumerConfig.GROUP_ID_CONFIG                 -> "group1",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  val inputTopicSet = Set("input")
  val messages = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
  )

  val lines = messages
    .map(_.value)
    .map(_.replace("\"", "").split(","))

  lines.foreachRDD { rdd =>
    val spark = SparkSession.builder
      .config(rdd.sparkContext.getConf)
      .getOrCreate()
    import spark.implicits._

    val dataDF = rdd
      .toDF("input")
      .withColumn("sepal_length", $"input" (0).cast(DoubleType))
      .withColumn("sepal_width", $"input" (1).cast(DoubleType))
      .withColumn("petal_length", $"input" (2).cast(DoubleType))
      .withColumn("petal_width", $"input" (3).cast(DoubleType))
      .drop("input")

    if (dataDF.count() > 0) {
      val vector_assembler = new VectorAssembler()
        .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
        .setOutputCol("features")

      val data: DataFrame       = vector_assembler.transform(dataDF)
      val prediction: DataFrame = model.transform(data)

      prediction
        .select(
          prediction_value_udf(col("prediction")).as("key"),
          concat_ws(
            ",",
            $"sepal_length",
            $"sepal_width",
            $"petal_length",
            $"petal_width",
            prediction_value_udf(col("prediction"))
          ).as("value")
        )
        .foreach { row =>
          kafkaSink.value.send("prediction", s"${row(1)}")
        }
    }
  }

  streamingContext.start()
  streamingContext.awaitTermination()
}
