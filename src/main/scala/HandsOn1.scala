import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HandsOn1 {
  def main(args: Array[String]) {

    val spark = getSpark()
    import spark.implicits._

    val moviesFile = "../../jigsaw/ml-latest-small/movies.csv"
    val tagsFile = "../../jigsaw/ml-latest-small/tags.csv"
    val ratingsFile = "../../jigsaw/ml-latest-small/ratings.csv"

    val moviesDF = loadDF(spark, moviesFile)

    // Count items in data frames
    // println(moviesDF.count())

    // Print Schema of the data frame
    // moviesDF.printSchema()

    // Show top 20 items in data frame
    // moviesDF.show()

    // Create new data frame using existing one
    moviesDF.select($"title").show()

    // Derived attributes
    // val titleDF = moviesDF.select($"title", length($"title").alias("length"))

    // Show top 3 items in data frame
    // titleDF.show(3)

    // Sort and get the first item and print it
    // println(titleDF.sort(col("length").desc).head)

    // Pattern matching, count and print it
    // println(titleDF.where($"title".rlike("[0-9]{4}")).count)

    // Source data clean up
    // val genresDF = moviesDF.select(split($"genres","\\|").alias("genres"))
    // genresDF.show(2)

    // Finding top 3 genres
    /*genresDF.select(explode($"genres").alias("genre"))
      .groupBy($"genre").count()
      .show(3)
    */
    // Find movies with most
    // val tagsDF = loadDF(spark, tagsFile)

    /*tagsDF.groupBy($"movieId").count()
      .sort($"count".desc).limit(3)
      .join(moviesDF, "movieId")
      .select("title", "count")
      .show()
    */
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
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
