import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

class ModelTraining(private val spark: SparkSession,
                    private val dataPath: String) {


  def training(fileName: String, outputFilePath: String): Unit = {

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val decision_tree_classifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline: Pipeline = new Pipeline().setStages(Array(decision_tree_classifier))

    val irisDF: DataFrame = spark.read
      .format("libsvm")
      .load(s"$dataPath/$fileName")

    val Array(train, test) = irisDF.randomSplit(Array(0.75, 0.25))
    val model = pipeline.fit(train)
    val predictions = model.transform(test)
    println("Accuracy: " + evaluator.evaluate(predictions).toString)

    model.write.overwrite().save(outputFilePath)
  }
}
