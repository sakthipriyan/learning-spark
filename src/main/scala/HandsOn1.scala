import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HandsOn1 {

  def main(args: Array[String]) {

    val spark = getSpark()
    import spark.implicits._

    val moviesFile = "../../../Downloads/ml-latest-small/movies.csv"
    val tagsFile = "../../../Downloads/ml-latest-small/tags.csv"
    val ratingsFile = "../../../Downloads/ml-latest-small/ratings.csv"

    val moviesDF = loadDF(spark, moviesFile)

    // 1. Count items in data frames
    // 9742





    println(moviesDF.count())






    /* 2. Print Schema of the data frame

    root
     |-- movieId: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- genres: string (nullable = true)
     */





    moviesDF.printSchema()






    /* 3. Show top 20 items in data frame
    +-------+--------------------+--------------------+
    |movieId|               title|              genres|
    +-------+--------------------+--------------------+
    |      1|    Toy Story (1995)|Adventure|Animati...|
    |      2|      Jumanji (1995)|Adventure|Childre...|
    |      3|Grumpier Old Men ...|      Comedy|Romance|
    |      4|Waiting to Exhale...|Comedy|Drama|Romance|
    |      5|Father of the Bri...|              Comedy|
    |      6|         Heat (1995)|Action|Crime|Thri...|
    |      7|      Sabrina (1995)|      Comedy|Romance|
    |      8| Tom and Huck (1995)|  Adventure|Children|
    |      9| Sudden Death (1995)|              Action|
    |     10|    GoldenEye (1995)|Action|Adventure|...|
    |     11|American Presiden...|Comedy|Drama|Romance|
    |     12|Dracula: Dead and...|       Comedy|Horror|
    |     13|        Balto (1995)|Adventure|Animati...|
    |     14|        Nixon (1995)|               Drama|
    |     15|Cutthroat Island ...|Action|Adventure|...|
    |     16|       Casino (1995)|         Crime|Drama|
    |     17|Sense and Sensibi...|       Drama|Romance|
    |     18|   Four Rooms (1995)|              Comedy|
    |     19|Ace Ventura: When...|              Comedy|
    |     20|  Money Train (1995)|Action|Comedy|Cri...|
    +-------+--------------------+--------------------+
    only showing top 20 rows
     */





    moviesDF.show()





    /* 4. Create new data frame using existing one

     */
    // Create new data frame using existing one
    moviesDF.select($"title").show()

    // Derived attributes
    val titleDF = moviesDF.select($"title", length($"title").alias("length"))

    // Show top 3 items in data frame
    titleDF.show(3)

    // Sort and get the first item and print it
    // println(titleDF.sort(col("length").desc).head)

    // Pattern matching, count and print it
    // println(titleDF.where($"title".rlike("[0-9]{4}")).count)

    // Source data clean up
    moviesDF.show(5)
    val genresDF = moviesDF.select(split($"genres","\\|").as("genres"))
    genresDF.printSchema()
    genresDF.show(5)

    // Finding top 3 genres
    genresDF.select(explode($"genres").alias("genre"))
      .groupBy($"genre").count()
      .sort($"count".desc)
      .show()

    // Find movies with most
    val tagsDF = loadDF(spark, tagsFile)

    tagsDF.groupBy($"movieId").count()
      .sort($"count".desc).limit(3)
      .join(moviesDF, "movieId")
      .select("title", "count")
      .show()

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
}
