import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, from_csv, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object StructuredStreaming extends App {

  val spark = SparkSession
    .builder()
    .appName("structure_streaming")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val resources_path        = "src/main/resources/"
  val model_path            = resources_path + "model"
  val training_path         = resources_path + "training"
  val iris_libsvm_file_name = "iris_libsvm.txt"

  if (args.length > 0 && args.head == "training") {
    val irisModelTraining = new ModelTraining(spark, training_path)
    irisModelTraining.training(iris_libsvm_file_name, model_path)
  }

  import spark.implicits._

  val iris_labels = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")

  val prediction_value_udf = udf((col: Double) => iris_labels(col))

  val iris_schema = StructType(
    StructField("sepal_length", DoubleType, nullable = true) ::
      StructField("sepal_width", DoubleType, nullable = true) ::
      StructField("petal_length", DoubleType, nullable = true) ::
      StructField("petal_width", DoubleType, nullable = true) ::
      Nil
  )

  val vector_assembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  val model = PipelineModel.load(model_path)

  val dataDF: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "input")
    .load()
    .select($"value".cast(StringType))
    .withColumn("struct", from_csv($"value", iris_schema, Map("sep" -> ",")))
    .withColumn("sepal_length", $"struct".getField("sepal_length"))
    .withColumn("sepal_width", $"struct".getField("sepal_width"))
    .withColumn("petal_length", $"struct".getField("petal_length"))
    .withColumn("petal_width", $"struct".getField("petal_width"))
    .drop("value", "struct")

  val data: DataFrame = vector_assembler.transform(dataDF)

  val prediction: DataFrame = model.transform(data)

  val query = prediction
    .withColumn(
      "predictedLabel",
      prediction_value_udf(col("prediction"))
    )
    .select(
      $"predictedLabel".as("key"),
      concat_ws(
        ",",
        $"sepal_length",
        $"sepal_width",
        $"petal_length",
        $"petal_width",
        $"predictedLabel"
      ).as("value")
    )
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("checkpointLocation", "src/main/resources/checkpoint/")
    .option("topic", "prediction")
    .start()

  query.awaitTermination
}
