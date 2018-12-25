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

    val moviesProcessedDF = moviesDF
      .withColumn("genres", split($"genres", "\\|"))
      .withColumn("genresCount", size($"genres"))

    val tagsProcessedDF = tagsDF
      .groupBy("movieId").agg(collect_set("tag").as("tags"))
      .withColumn("tagCount", size($"tags"))

    val ratingsProcessedDF = ratingsDF
      .groupBy($"movieId")
      .agg(avg("rating").as("ratingAvg"),
        count("rating").as("ratingCount"))

    val row = ratingsProcessedDF.agg(avg($"ratingAvg"), avg($"ratingCount")).first
    val avgRating = row.getDouble(0)
    val avgCount = row.getDouble(1)

    val moviesLabelDF = ratingsProcessedDF.select($"movieId",
      when($"ratingCount" >= avgCount && $"ratingAvg" >= avgRating, 0.0)
        .when($"ratingCount" >= avgCount && $"ratingAvg" < avgRating, 1.0)
        .when($"ratingCount" < avgCount && $"ratingAvg" >= avgRating, 2.0)
        .otherwise(3.0).as("label"))

    val moviesConsolidatedDF = moviesProcessedDF
      .join(tagsProcessedDF, "movieId")
      .join(moviesLabelDF, "movieId")

    val moviesMergedDF = moviesConsolidatedDF
      .withColumn("genres", arrayToMap($"genres"))
      .withColumn("tags", arrayToMap($"tags"))

    val moviesFeaturedDF = moviesMergedDF.select(
      $"movieId",
      $"title",
      $"label",
      $"genresCount".as("gCount"),
      $"tagCount".as("tCount"),
      $"genres.Drama",
      $"genres.Comedy",
      $"genres.Romance",
      $"genres.Thriller",
      $"genres.Action",
      $"genres.Adventure",
      $"genres.Crime",
      $"genres.Sci-Fi",
      $"genres.Mystery",
      $"genres.Fantasy",
      $"genres.Children",
      $"genres.Horror",
      $"genres.Animation",
      $"genres.Musical",
      $"genres.War",
      $"genres.Documentary",
      $"genres.Film-Noir",
      $"genres.IMAX",
      $"genres.Western",
      $"tags.In Netflix queue",
      $"tags.atmospheric",
      $"tags.superhero",
      $"tags.Disney",
      $"tags.religion",
      $"tags.funny",
      $"tags.quirky",
      $"tags.surreal",
      $"tags.psychology",
      $"tags.thought-provoking",
      $"tags.crime".as("tCrime"),
      $"tags.suspense",
      $"tags.politics",
      $"tags.visually appealing",
      $"tags.sci-fi".as("tSci-fi"),
      $"tags.dark comedy",
      $"tags.twist ending",
      $"tags.dark",
      $"tags.mental illness",
      $"tags.comedy".as("tComedy")
    ).na.fill(0.0)

    storeDF(moviesFeaturedDF.repartition(1), "../../../Downloads/ml-latest-small/movies-featured.csv")

    spark.stop()
  }

  def loadDF(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  def storeDF(dataFrame: DataFrame, path: String) = {
    dataFrame.write.option("header", "true").csv(path)
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
