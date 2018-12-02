import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.VectorAssembler

object HandsOn2 {

  def main(args: Array[String]) {

    // Get Spark
    val spark = getSpark()
    import spark.implicits._

    val moviesFile = "../../../Downloads/ml-latest-small/movies.csv"
    val tagsFile = "../../../Downloads/ml-latest-small/tags.csv"
    val ratingsFile = "../../../Downloads/ml-latest-small/ratings.csv"

    val moviesDF = loadDF(spark, moviesFile)
    val tagsDF = loadDF(spark, tagsFile)
    val ratingsDF = loadDF(spark, ratingsFile)

    // Feature Engineering
    // val moviesProcessedDF
    // val tagsProcessedDF
    // val ratingsProcessedDF
    // val row
    // val avgRating
    // val avgCount
    // val moviesLabelDF
    // val moviesConsolidatedDF
    // val moviesMapDF
    // val moviesFeaturedDF
    // moviesFeaturedDF.show(1)
    // val features

    // Data Transformation
    // val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    // val data = assembler.transform(moviesFeaturedDF).select("movieId","features","label")
    // data.show()

    // Split the data into train and test
    // val splits = data.randomSplit(Array(0.8, 0.2), seed = 1234L)
    // val train = splits(0)
    // val test = splits(1)

    // Setting up Hyper Parameters
    // val layers = Array[Int](features.length, features.length + 2, features.length + 1, 4)

    // Train the model using train data and test it on the test data.
    // val trainer
    // val model

    // Run the model
    // val result

    // Find the accuracy metrics
    // val predictionAndLabels = result.select("prediction", "label")
    // val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    // println(s"Features: ${features.mkString(", ")}. Test set accuracy = ${(100 * evaluator.evaluate(predictionAndLabels)).formatted("%.2f")}%")

    spark.stop()
  }

  def loadDF(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  def getSpark() = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // Create UDF converting Array[String] to Map[String, Double]
  val arrayToMap = udf[Map[String, Double], Seq[String]] {
    element => element.map { case key: String => (key, 1.0) }.toMap
  }
}
