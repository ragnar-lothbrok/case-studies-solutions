package com.simplilearn.bigdata.january_casestudy_3

import com.amazonaws.regions.Regions
import com.mongodb.spark.MongoSpark
import com.simplilearn.bigdata.january_casestudy_1.UDFUtils
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * Youtube Video Analytics
 */
object Solution_1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      System.out.println("Please provide <input_files> <writeS3> <writeMongo> <bucket> <spark_master>")
      System.exit(0)
    }

    val input_files: String = args(0)
    val writeToS3: Boolean = args(1).toBoolean
    val writeToMongo: Boolean = args(2).toBoolean
    val bucket: String = args(3)


    val sparkSession = getSparkSession("ECommerce-analysis", args(4))
    val dataset = readFile(input_files, readWithHeader(dataSchema(), sparkSession))
    val modifiedDataset = filterAndModify(dataset)

    val timeBucketList = List("Day", "Week")
    for (timeBucket <- timeBucketList) {
      solution1(modifiedDataset, writeToS3, writeToMongo, bucket, timeBucket)
    }

    totalFreightCharges(modifiedDataset, writeToS3, writeToMongo, bucket)
  }

  def totalFreightCharges(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String) = {
    var modifiedDataset = dataset
      .select("customer_city", "order_freight_value")
      .groupBy("customer_city")
      .agg(
        functions.sum("order_freight_value").as("totalFreightValuePerCity"))
      .drop("order_freight_value")
      .sort(functions.desc("customer_city"))

    /**
     * * Freight charges distribution in each Customer City.
     */
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "total_freight_value_per_city", "Total")

    modifiedDataset = modifiedDataset.select("totalFreightValuePerCity").agg(functions.sum("totalFreightValuePerCity").as("totalFreightValue")).drop("totalFreightValuePerCity")

    /**
     * Total Freight charges.
     */
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "total_freight_value", "Total")
  }

  /**
   * a.	SALES
   * •	Total sales.
   * •	Total Sales in each Customer City.
   * •	Total sales in each Customer State.
   * b.	ORDERS
   * •	Total number of orders sold.
   * •	City wise order distribution.
   * •	State wise order distribution.
   * •	Average Review score per Order.
   * •	Average Freight charges per order.
   * •	Average time taken to approve the orders. (Order Approved – Order Purchased).
   * •	Average order delivery time.
   *
   * @param dataset
   * @param writeToS3
   * @param writeToMongo
   * @param bucket
   */
  def solution1(dataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String, timeBucket: String) = {

    var modifiedDataset = dataset
      .select(timeBucket, "customer_city", "customer_state", "order_products_value")
      .groupBy(timeBucket, "customer_city", "customer_state")
      .agg(
        functions.sum("order_products_value").as("totalSalesCitywise"),
        functions.count("*").as("totalOrderCityWise"))
      .drop("order_products_value")
      .sort(functions.desc(timeBucket))

    /**
     * Total sales in each Customer City.
     * City wise order distribution.
     */
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "day_sales_order_city", timeBucket)


    modifiedDataset = modifiedDataset.select(timeBucket, "customer_state", "totalSalesCitywise", "totalOrderCityWise")
      .groupBy(timeBucket, "customer_state")
      .agg(
        functions.sum("totalSalesCitywise").as("totalSalesStatewise"),
        functions.sum("totalOrderCityWise").as("totalOrderStateWise"))
      .drop("totalSalesCitywise", "totalOrderCityWise")
      .sort(functions.desc(timeBucket))

    /**
     * Total sales in each Customer State.
     * State wise order distribution.
     */
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "day_sales_order_state", timeBucket)


    modifiedDataset = dataset
      .select(timeBucket)
      .groupBy(timeBucket)
      .agg(
        functions.count("*").as("totalOrders"))

    /**
     * Total number of orders sold.
     */
    write(modifiedDataset, writeToS3, writeToMongo, bucket, "total_orders_per_day", timeBucket)


    /**
     * Average Review score per Order.
     * Average Freight charges per order.
     * Average time taken to approve the orders. (Order Approved – Order Purchased).
     * Average order delivery time.
     */
    modifiedDataset = dataset
      .select(timeBucket, "review_score", "order_freight_value", "order_purchase_timestamp", "order_aproved_at", "order_delivered_customer_date")
      .withColumn("timetakentoapprove", UDFUtils.timeTakenToApprove(dataset("order_purchase_timestamp"), dataset("order_aproved_at")))
      .withColumn("timetakentodeliver", UDFUtils.timeTakenToApprove(dataset("order_purchase_timestamp"), dataset("order_delivered_customer_date")))
      .groupBy(timeBucket)
      .agg(
        functions.avg("review_score").as("avgReviewScorePerDay"),
        functions.avg("order_freight_value").as("avgFreightValuePerDay"),
        functions.count("*").as("totalOrdersPerDay"),
        functions.avg("timetakentoapprove").as("avgOrderApprovalTimeMS"),
        functions.avg("timetakentodeliver").as("avgOrderDeliverTimeMS"))
      .drop("review_score", "order_delivered_customer_date", "order_aproved_at", "order_delivered_customer_date")
      .sort(functions.desc(timeBucket))

    modifiedDataset = modifiedDataset
      .withColumn("avgOrderApprovalTimeMS", UDFUtils.longToString(modifiedDataset("avgOrderApprovalTimeMS")))
      .withColumn("avgOrderDeliverTimeMS", UDFUtils.longToString(modifiedDataset("avgOrderDeliverTimeMS")))

    write(modifiedDataset, writeToS3, writeToMongo, bucket, "avgreview_freightv_approveorder_delivertime", timeBucket)
  }


  def write(modifiedDataset: Dataset[Row], writeToS3: Boolean, writeToMongo: Boolean, bucket: String, name: String, timeBucket: String) = {
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("/tmp/solution2/" + bucket + "/" + timeBucket + "/" + name)
    print("Output created in local tmp directory /tmp/solution2/" + bucket)

    if (writeToMongo) {
      MongoSpark.save(modifiedDataset)
      print("Data Pushed to Mongo")
    }

    if (writeToS3) {
      modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("s3a://" + bucket + "/solution2/")
      print("Data Pushed to S3.")
    }
  }


  def filterAndModify(dataset: Dataset[Row]) = {
    val modifiedDataset = dataset.withColumn("Week", UDFUtils.getWeekFromString(dataset("order_purchase_timestamp")))
      .withColumn("Day", UDFUtils.getDayFromString(dataset("order_purchase_timestamp")))
    modifiedDataset
  }


  def getSparkSession(appName: String, master: String) = {
    //    val username = System.getenv("MONGOUSERNAME")
    //    val password = System.getenv("MONGOPASSWORD")
    //    val uri: String = "mongodb://"+username+":"+password+"@cluster0-shard-00-00-50m8b.mongodb.net:27017,cluster0-shard-00-01-50m8b.mongodb.net:27017,cluster0-shard-00-02-50m8b.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority"
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master)
      //      .config("spark.mongodb.output.uri", uri)
      //      .config("spark.mongodb.output.collection", "ecommerce-"+Calendar.getInstance.getTimeInMillis)
      .getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
        hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
        hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
        hadoopConf.set("fs.s3a.endpoint", "s3."+Regions.US_WEST_2.getName+".amazonaws.com")
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
      StructField("id", StringType, true),
      StructField("order_status", StringType, true),
      StructField("order_products_value", FloatType, true),
      StructField("order_freight_value", FloatType, true),
      StructField("order_items_qty", IntegerType, true),
      StructField("customer_city", StringType, true),
      StructField("customer_state", StringType, true),
      StructField("customer_zip_code_prefix", StringType, true),
      StructField("product_name_lenght", IntegerType, true),
      StructField("product_description_lenght", IntegerType, true),
      StructField("product_photos_qty", IntegerType, true),
      StructField("review_score", IntegerType, true),
      StructField("order_purchase_timestamp", StringType, true),
      StructField("order_aproved_at", StringType, true),
      StructField("order_delivered_customer_date", StringType, true)
    ))
  }

  def readWithHeader(schema: StructType, sparkSession: SparkSession) = {
    sparkSession
      .read
      .option("header", false)
      .schema(schema).option("mode", "DROPMALFORMED")
  }
}
