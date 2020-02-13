package com.simplilearn.bigdata.january_casestudy_3

import com.amazonaws.regions.Regions
import com.mongodb.spark.MongoSpark
import com.simplilearn.bigdata.january_casestudy_1.UDFUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/**
 * Youtube Video Analytics
 */
object Solution_3 {

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      System.out.println("Please provide <category_tltle_csv> <us_videos_csv> <writeS3> <writeMongo> <bucket> <spark_master>")
      System.exit(0)
    }

    val categoryTitlePath: String = args(0)
    val usVideosPath: String = args(1)
    val writeToS3: Boolean = args(2).toBoolean
    val writeToMongo: Boolean = args(3).toBoolean
    val bucket: String = args(4)


    val sparkSession = getSparkSession("Youtube-Analysis", args(5))
    val categoryTitleDataset = readFile(categoryTitlePath, readWithHeader(categorySchema(), sparkSession))
    val usVideosDataset = readFile(usVideosPath, readWithHeader(dataSchema(), sparkSession))

    val modifiedDataset = usVideosDataset.withColumn("Year", UDFUtils.getYearFromString(usVideosDataset("trending_date")))
      .withColumn("Month", UDFUtils.getMonthFromString(usVideosDataset("trending_date")))
      .withColumn("timestamp", UDFUtils.getTimeFromString(usVideosDataset("trending_date"))).cache()

    //    solution1(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution2(modifiedDataset, writeToS3, writeToMongo, bucket)
  }

  def solution1(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val condition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val cachedDataset = dataset
      .withColumn("row", functions.row_number.over(condition))
      .where("row==1").drop("row")

    //Top 3 videos for which user interaction (views + likes + dislikes + comments) is highest
    var modifiedDataset = cachedDataset
      .select("video_id", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(dataset("views"), dataset("likes"), dataset("dislikes"), dataset("comment_count")))
      .drop("views", "likes", "dislikes", "comment_count")
      .orderBy(functions.desc("totalInteractions"))
      .limit(3)
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "videoshighestinteraction")


    //Top 3 videos for which user interaction (views + likes + dislikes + comments) is lowest
    modifiedDataset = dataset
      .select("video_id", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(dataset("views"), dataset("likes"), dataset("dislikes"), dataset("comment_count")))
      .drop("views", "likes", "dislikes", "comment_count")
      .orderBy(functions.asc("totalInteractions"))
      .limit(3)
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "videoslowestinteraction")
  }

  /**
   * •	Top 3 video of each category on each year
   *
   * By number of views
   * By number of comments
   * By number of likes
   * User interaction is highest.
   */
  def solution2(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {


    val condition = Window.partitionBy("year", "video_id").orderBy(functions.desc("timestamp"))
    val cachedDataset = dataset
      .withColumn("row", functions.row_number.over(condition))
      .where("row==1").drop("row")


    //By number of views
    val viewsCondition = Window.partitionBy("category_id", "Year").orderBy(functions.desc("views"))
    val result1 = cachedDataset
      .select("video_id", "Year", "category_id", "timestamp", "views", "trending_date")
      .withColumn("row", functions.row_number.over(viewsCondition))
      .where("row==1 OR row==2 OR row==3").drop("row")
        .orderBy(functions.desc("category_id"), functions.desc("video_id"), functions.desc("views"))
    result1.show(10)
    write(result1, writeToS3, writeToMongo, bucket, "videosbyviewsbycategory")

    //By number of comments
    val commentsCondition = Window.partitionBy("category_id", "Year").orderBy(functions.desc("comment_count"))
    val result2 = cachedDataset
      .select("video_id", "Year","category_id", "timestamp", "comment_count", "trending_date")
      .withColumn("row", functions.row_number.over(commentsCondition))
      .where("row==1 OR row==2 OR row==3").drop("row")
      .orderBy(functions.desc("category_id"), functions.desc("video_id"), functions.desc("comment_count"))
    write(result2, writeToS3, writeToMongo, bucket, "videosbycommentsbycategory")

    //By number of likes
    val likesCondition = Window.partitionBy("category_id", "Year").orderBy(functions.desc("likes"))
    val result3 = cachedDataset
      .select("video_id", "Year","category_id", "timestamp", "likes", "trending_date")
      .withColumn("row", functions.row_number.over(likesCondition))
      .where("row==1 OR row==2 OR row==3").drop("row")
      .orderBy(functions.desc("category_id"), functions.desc("video_id"), functions.desc("likes"))
    write(result3, writeToS3, writeToMongo, bucket, "videosbylikesbycategory")

    //By number of likes
    val totalInteractionsCondition = Window.partitionBy("category_id", "Year").orderBy(functions.desc("totalInteractions"))
    val result4 = cachedDataset
      .select("video_id", "Year","category_id", "timestamp", "trending_date", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(dataset("views"), dataset("likes"), dataset("dislikes"), dataset("comment_count")))
      .drop("views", "likes", "dislikes", "comment_count")
      .withColumn("row", functions.row_number.over(totalInteractionsCondition))
      .where("row==1 OR row==2 OR row==3").drop("row")
      .orderBy(functions.desc("category_id"), functions.desc("video_id"), functions.desc("totalInteractions"))
    write(result4, writeToS3, writeToMongo, bucket, "videosbyinteractionbycategory")

  }


  def write(modifiedDataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String, name: String) = {
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("/tmp/solution3/" + bucket + "/" + name)
    print("Output created in local tmp directory /tmp/solution3/" + bucket)

    if (writeToMongo) {
      MongoSpark.save(modifiedDataset)
      print("Data Pushed to Mongo")
    }

    if (writeToS3) {
      modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("s3a://" + bucket + "/solution3/" + name)
      print("Data Pushed to S3.")
    }
  }

  def getSparkSession(appName: String, master: String) = {
    val username = System.getenv("MONGOUSERNAME")
    val password = System.getenv("MONGOPASSWORD")
    val server = System.getenv("MONGOSERVER")
    val uri: String = "mongodb://" + username + ":" + password + "@" + server + ":27017"
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master)
      .config("spark.mongodb.output.uri", uri)
      .config("spark.mongodb.output.collection", "youtube")
      .config("spark.mongodb.output.database", "test")
      .getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
    hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoopConf.set("fs.s3a.endpoint", "s3." + Regions.US_WEST_2.getName + ".amazonaws.com")
    sparkSession
  }

  def readFile(path: String, dataFrameReader: DataFrameReader) = {
    System.out.println("Reading file " + path)
    val dataset = dataFrameReader.csv(path)
    System.out.println("Dataset Schema " + dataset.schema)
    System.out.println("Row Count" + dataset.count())
    dataset
  }

  def dataSchema() = {
    StructType(Array(
      StructField("video_id", StringType, true),
      StructField("trending_date", StringType, true),
      StructField("title", StringType, true),
      StructField("channel_title", StringType, true),
      StructField("category_id", IntegerType, true),
      StructField("publish_time", StringType, true),
      StructField("tags", StringType, true),
      StructField("views", LongType, true),
      StructField("likes", LongType, true),
      StructField("dislikes", LongType, true),
      StructField("comment_count", LongType, true),
      StructField("comments_disabled", StringType, true),
      StructField("ratings_disabled", StringType, true)
    ))
  }

  def categorySchema() = {
    StructType(Array(
      StructField("category", IntegerType, true),
      StructField("title", StringType, true)
    ))
  }

  def readWithHeader(schema: StructType, sparkSession: SparkSession) = {
    sparkSession
      .read
      .option("header", false)
      .schema(schema).option("mode", "DROPMALFORMED")
  }
}