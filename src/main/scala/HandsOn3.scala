import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

object HandsOn3 {

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

  def getFeatures(features: Array[String]): List[Array[String]] = {
    features.toSet.subsets().toList.filter(set => set.size > 0).map(set => set.toArray)
  }

  def main(args: Array[String]): Unit = {

    // Init spark and load dataFrame
    val spark = getSpark()
    val moviesFeaturedFile = "../../../Downloads/ml-latest-small/movies-featured.csv"
    val moviesFeaturedDF = loadDF(spark,moviesFeaturedFile)

    // Select set of features
    val featuresArray = Array("gCount", "tCount", "Drama", "Comedy", "Romance", "Thriller", "Action")

    for (features <- getFeatures(featuresArray)) {

      val (train,test) = trainAndTest(moviesFeaturedDF, features)

      // Train Model using train data for the selected features.
      val mlpcModel =  trainMLPC(train,features)

      // Apply trained model on the test and predict the class.
      val resultMLPC = mlpcModel.transform(test)

      // Print the accuracy of the model.
      printAccuracy(resultMLPC, features)
    }
  }

  def printAccuracy(dataFrame: DataFrame, features: Array[String]): Unit = {
    val predictionAndLabels = dataFrame.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    println("Accuracy = " +
      s"${(100 * evaluator.evaluate(predictionAndLabels)).formatted("%.2f")}%" +
      s" Features: ${features.mkString(", ")}. ")
  }

  def trainAndTest(df: DataFrame, features: Array[String]): (DataFrame, DataFrame) = {

    // Data Transformation
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    val data = assembler.transform(df).select("movieId", "features", "label")

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    (train, test)
  }

  def trainRFC(train: DataFrame, features: Array[String]): RandomForestClassificationModel = {
    val trainer = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    // This would train the RandomForestClassifier for the given train data and hyper parameters.
    trainer.fit(train)
  }

  def trainMLPC(train: DataFrame, features: Array[String]): MultilayerPerceptronClassificationModel = {

    // Setting up Hyper Parameters. Specify layers for the neural network:
    // Input layer of size (features.length),
    // Two intermediate of size (features.length + 2) and (features.length + 1)
    // Output of size 4 (classes)
    val layers = Array[Int](features.length, features.length + 2, features.length + 1, 4)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // This would train the MultilayerPerceptronClassifier for the given data and hyper parameters.
    trainer.fit(train)
  }

}
