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

    if (args.length != 9) {
      System.out.println("Please provide <category_tltle_csv> <us_videos_csv> <writeS3> <writeMongo> <bucket> <spark_master> <mongousername> <mongopasswprd> <mongoserver>")
      System.exit(0)
    }

    val categoryTitlePath: String = args(0)
    val usVideosPath: String = args(1)
    val writeToS3: Boolean = args(2).toBoolean
    val writeToMongo: Boolean = args(3).toBoolean
    val bucket: String = args(4)


    val sparkSession = getSparkSession("Youtube-Analysis", args(5), args(6), args(7), args(8))
    val categoryTitleDataset = readFile(categoryTitlePath, readWithHeader(categorySchema(), sparkSession))
    val usVideosDataset = readFile(usVideosPath, readWithHeader(dataSchema(), sparkSession))

    val modifiedDataset = usVideosDataset.withColumn("Year", UDFUtils.getYearFromString(usVideosDataset("trending_date")))
      .withColumn("Month", UDFUtils.getMonthFromString(usVideosDataset("trending_date")))
      .withColumn("timestamp", UDFUtils.getTimeFromString(usVideosDataset("trending_date"))).cache()

    solution1(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution2(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution3(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution4(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution5(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution6(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution7(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution8(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution9(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution10(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution11(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution12(modifiedDataset, writeToS3, writeToMongo, bucket)
    solution13(modifiedDataset, writeToS3, writeToMongo, bucket)
  }

  /**
   *	o	All videos belong to a specific category
   */
  def solution11(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("category_id", "video_id")
      .groupBy("category_id")
      .agg(functions.collect_list("video_id").as("allvideos"))
      .drop("video_id")
      .orderBy(functions.desc("category_id"))
    write(result1, writeToS3, writeToMongo, bucket, "videosofcategory")
  }

  /**
   *	All videos belong to any given channel
   */
  def solution12(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("channel_title", "video_id")
      .groupBy("channel_title")
      .agg(functions.collect_list("video_id").as("allvideos"))
      .drop("video_id")
      .orderBy(functions.desc("channel_title"))
    write(result1, writeToS3, writeToMongo, bucket, "videosofchannel")
  }

  /**
   *	All unique videos with title
   */
  def solution13(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("title", "video_id")
    write(result1, writeToS3, writeToMongo, bucket, "videosandIds")
  }

  /**
   * •	Top 3 channels
   *
   * o	By number of Total views
   * o	Like/Dislike ratio is highest
   * o	By number of Total Comments
   */
  def solution5(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val filterColumns = List("views", "comment_count")
    for (filterColumn <- filterColumns) {
      val result1 = uniqueVideoDataset
        .select("channel_title", filterColumn)
        .groupBy("channel_title")
        .agg(functions.sum(filterColumn).as("total" + filterColumn))
        .orderBy(functions.desc("total" + filterColumn)).limit(3)
      write(result1, writeToS3, writeToMongo, bucket, "channelbytotal" + filterColumn)
    }

    {
      var result1 = uniqueVideoDataset
        .select("channel_title", "likes", "dislikes")
        .groupBy("channel_title")
        .agg(functions.sum("likes").as("totalLikes"), functions.sum("dislikes").as("totalDislikes"))


      result1 = result1.withColumn("likeDislikeRatio", UDFUtils.likeDislikeRatio(functions.col("totalLikes"), functions.col("totalDislikes")))
        .orderBy(functions.desc("likeDislikeRatio")).limit(3)
      write(result1, writeToS3, writeToMongo, bucket, "channelbylikeDislikeRatio")
    }
  }

  /**
   * •	Top 3 Categories
   *
   * o	By number of Total views
   * o	Like/Dislike ratio is highest
   * o	By number of Total Comments
   */
  def solution6(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val filterColumns = List("views", "comment_count")
    for (filterColumn <- filterColumns) {
      val result1 = uniqueVideoDataset
        .select("category_id", filterColumn)
        .groupBy("category_id")
        .agg(functions.sum(filterColumn).as("total" + filterColumn))
        .orderBy(functions.desc("total" + filterColumn)).limit(3)
      write(result1, writeToS3, writeToMongo, bucket, "categorybytotal" + filterColumn)
    }

    {
      var result1 = uniqueVideoDataset
        .select("category_id", "likes", "dislikes")
        .groupBy("category_id")
        .agg(functions.sum("likes").as("totalLikes"), functions.sum("dislikes").as("totalDislikes"))
      result1 = result1.withColumn("likeDislikeRatio", UDFUtils.likeDislikeRatio(functions.col("totalLikes"), functions.col("totalDislikes")))
        .orderBy(functions.desc("likeDislikeRatio")).limit(3)
      write(result1, writeToS3, writeToMongo, bucket, "categorybylikeDislikeRatio")
    }
  }

  /**
   * •	Top 3 videos
   *
   * o	Value calculated by below formula is highest
   * 	 (Views on most recent date / (Recent Date - Publish Date))
   */

  def solution7(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("video_id", "timestamp", "publish_time", "views")
      .withColumn("score", UDFUtils.viewsPerDuration(uniqueVideoDataset("views"), uniqueVideoDataset("timestamp"), uniqueVideoDataset("publish_time")))
      .drop("timestamp", "publish_time", "views")
      .orderBy(functions.desc("score")).limit(3)
    write(result1, writeToS3, writeToMongo, bucket, "mostviewsinlessduration")
  }

  /**
   * Calculate any 3 videos which got at least 5 comments on every 1000 views.
   *
   * @param dataset
   * @param writeToS3
   * @param writeToMongo
   * @param bucket
   */
  def solution8(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("video_id", "comment_count", "views")
      .withColumn("score", UDFUtils.commentPerViews(uniqueVideoDataset("comment_count"), uniqueVideoDataset("views")))
      .drop("comment_count", "publish_time", "views")
      .orderBy(functions.desc("score")).limit(3)
    write(result1, writeToS3, writeToMongo, bucket, "mostcommentsperviews")
  }

  /**
   * Calculate any 3 videos which got at least 4 likes on every 100 views.
   *
   * @param dataset
   * @param writeToS3
   * @param writeToMongo
   * @param bucket
   */
  def solution9(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("video_id", "likes", "views")
      .withColumn("score", UDFUtils.likesPerViews(uniqueVideoDataset("likes"), uniqueVideoDataset("views")))
      .drop("likes", "publish_time", "views")
      .orderBy(functions.desc("score")).limit(3)
    write(result1, writeToS3, writeToMongo, bucket, "mostlikessperviews")
  }

  /**
   * •	Number of videos published in each category.
   *
   * @param dataset
   * @param writeToS3
   * @param writeToMongo
   * @param bucket
   */
  def solution10(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val latestVideoDataCondition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val result1 = uniqueVideoDataset
      .select("category_id")
      .groupBy("category_id")
      .agg(functions.count("*").as("totalVideos"))
      .orderBy(functions.desc("totalVideos"))
    write(result1, writeToS3, writeToMongo, bucket, "videoscountpercategory")
  }

  def getSparkSession(appName: String, master: String, username:String, password:String, server:String) = {
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

  def solution1(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val condition = Window.partitionBy("video_id").orderBy(functions.desc("timestamp"))
    val cachedDataset = dataset
      .withColumn("row", functions.row_number.over(condition))
      .where("row==1").drop("row")

    //Top 3 videos for which user interaction (views + likes + dislikes + comments) is highest
    var modifiedDataset = cachedDataset
      .select("video_id", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(cachedDataset("views"), cachedDataset("likes"), cachedDataset("dislikes"), cachedDataset("comment_count")))
      .drop("views", "likes", "dislikes", "comment_count")
      .orderBy(functions.desc("totalInteractions"))
      .limit(3)
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "videoshighestinteraction")


    //Top 3 videos for which user interaction (views + likes + dislikes + comments) is lowest
    modifiedDataset = cachedDataset
      .select("video_id", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(cachedDataset("views"), cachedDataset("likes"), cachedDataset("dislikes"), cachedDataset("comment_count")))
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

    val uniqueVideoFilter = "year"
    val latestVideoDataCondition = Window.partitionBy(uniqueVideoFilter, "video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val filterColumns = List("views", "comment_count", "likes")
    for (filterColumn <- filterColumns) {
      val filterCondition = Window.partitionBy("category_id", uniqueVideoFilter).orderBy(functions.desc(filterColumn))
      val result1 = uniqueVideoDataset
        .select("video_id", uniqueVideoFilter, "category_id", "timestamp", filterColumn, "trending_date")
        .withColumn("row", functions.row_number.over(filterCondition))
        .where("row==1 OR row==2 OR row==3").drop("row")
        .orderBy(functions.desc("category_id"), functions.desc(uniqueVideoFilter), functions.desc("views"))
      write(result1, writeToS3, writeToMongo, bucket, "videosby" + filterColumn + "bycategoryyearly")
    }


    //By number of interactions
    val totalInteractionsCondition = Window.partitionBy("category_id", "Year").orderBy(functions.desc("totalInteractions"))
    val result4 = uniqueVideoDataset
      .select("video_id", "Year", "category_id", "timestamp", "trending_date", "views", "likes", "dislikes", "comment_count")
      .withColumn("totalInteractions", UDFUtils.totalInteractions(uniqueVideoDataset("views"), uniqueVideoDataset("likes"), uniqueVideoDataset("dislikes"), uniqueVideoDataset("comment_count")))
      .drop("views", "likes", "dislikes", "comment_count")
      .withColumn("row", functions.row_number.over(totalInteractionsCondition))
      .where("row==1 OR row==2 OR row==3").drop("row")
      .orderBy(functions.desc("category_id"), functions.desc("video_id"), functions.desc("totalInteractions"))
    write(result4, writeToS3, writeToMongo, bucket, "videosbyinteractionbycategory")

  }

  /**
   * •	Top 3 video of each category on each month
   * o	By number of views
   * o	By number of likes
   * o	By number of dislikes
   */
  def solution3(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val uniqueVideoFilter = "month"
    val latestVideoDataCondition = Window.partitionBy(uniqueVideoFilter, "video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")

    val filterColumns = List("views", "dislikes", "likes")
    for (filterColumn <- filterColumns) {
      val filterCondition = Window.partitionBy("category_id", uniqueVideoFilter).orderBy(functions.desc(filterColumn))
      val result1 = uniqueVideoDataset
        .select("video_id", uniqueVideoFilter, "category_id", "timestamp", filterColumn, "trending_date")
        .withColumn("row", functions.row_number.over(filterCondition))
        .where("row==1 OR row==2 OR row==3").drop("row")
        .orderBy(functions.desc("category_id"), functions.desc(uniqueVideoFilter), functions.desc("views"))
      write(result1, writeToS3, writeToMongo, bucket, "videosby" + filterColumn + "bycategorymonthly")
    }
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

  /**
   * •	Top 3 video on each month
   * o	Like/Dislike ratio is highest
   */
  def solution4(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String): Unit = {

    val uniqueVideoFilter = "month"
    val latestVideoDataCondition = Window.partitionBy(uniqueVideoFilter, "video_id").orderBy(functions.desc("timestamp"))
    val uniqueVideoDataset = dataset
      .withColumn("row", functions.row_number.over(latestVideoDataCondition))
      .where("row==1").drop("row")
      .withColumn("likeDislikeRatio", UDFUtils.likeDislikeRatio(dataset("likes"), dataset("dislikes")))

    val filterColumns = List("likeDislikeRatio")
    for (filterColumn <- filterColumns) {
      val filterCondition = Window.partitionBy(uniqueVideoFilter).orderBy(functions.desc(filterColumn))
      val result1 = uniqueVideoDataset
        .select("video_id", uniqueVideoFilter, "timestamp", filterColumn, "trending_date")
        .withColumn("row", functions.row_number.over(filterCondition))
        .where("row==1 OR row==2 OR row==3").drop("row")
        .orderBy(functions.desc(uniqueVideoFilter), functions.desc(filterColumn))
      val path = "videosby" + filterColumn + "bymonth"
      write(result1, writeToS3, writeToMongo, bucket, path)
    }
  }
}
