package com.simplilearn.bigdata

import com.amazonaws.regions.Regions
import com.mongodb.spark.MongoSpark
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * E- Commerce Analytics
 */
object TestMongo {

  def main(args: Array[String]): Unit = {

    val bucket = args(0)
    val path = args(1)

    val sparkSession = getSparkSession("ECommerce-analysis", "local")
    val modifiedDataset = readFile(path, readWithHeader(dataSchema(), sparkSession))
    MongoSpark.save(modifiedDataset);
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("s3a://" + bucket + "/dummy/")
    print("Data Pushed to S3.")

  }

  def getSparkSession(appName: String, master: String) = {
    val username = System.getenv("MONGOUSERNAME")
    val password = System.getenv("MONGOPASSWORD")
    val server   = System.getenv("MONGOSERVER")
    val uri: String = "mongodb://" + username + ":" + password + "@" + server + ":27017"
    print("==== "+ uri)
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master)
      .config("spark.mongodb.output.uri", uri)
      .config("spark.mongodb.output.collection", "ecommerce")
      .config("spark.mongodb.output.database", "test")
      .getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
    hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    hadoopConf.set("fs.s3a.endpoint", "s3." + Regions.US_WEST_2.getName + ".amazonaws.com")
    hadoopConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    sparkSession
  }

  def readFile(path: String, dataFrameReader: DataFrameReader) = {
    System.out.println("Reading file " + path)
    val dataset = dataFrameReader.csv(path)
    println("Dataset Schema " + dataset.schema)
    println("Row Count" + dataset.count())
    dataset
  }

  def dataSchema() = {
    StructType(Array(
      StructField("City", StringType, true),
      StructField("Country", StringType, true)
    ))
  }

  def readWithHeader(schema: StructType, sparkSession: SparkSession) = {
    sparkSession
      .read
      .option("header", false)
      .schema(schema).option("mode", "DROPMALFORMED")
  }
}
