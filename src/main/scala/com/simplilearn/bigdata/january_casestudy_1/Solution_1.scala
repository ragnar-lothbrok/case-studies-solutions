package com.simplilearn.bigdata.january_casestudy_1

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Solution_1 {

  def main(args: Array[String]): Unit = {

    val city_attributes: String = "/Users/labuser/Downloads/solution_1/city_attributes.csv"
    val pressure: String = "/Users/labuser/Downloads/solution_1/pressure.csv"
    val humidity: String = "/Users/labuser/Downloads/solution_1/humidity.csv"
    val temperature: String = "/Users/labuser/Downloads/solution_1/temperature.csv"
    val weather_description: String = "/Users/labuser/Downloads/solution_1/weather_description.csv"
    val wind_direction: String = "/Users/labuser/Downloads/solution_1/wind_direction.csv"
    val wind_speed: String = "/Users/labuser/Downloads/solution_1/wind_speed.csv"

    val sparkSession = getSparkSession("weather-analysis", "local")
    val dataset = readFile(city_attributes, readWithHeader(citySchema(), sparkSession))

    createCityMap(dataset)

    var pressureDataset = filterAndModify(readFile(pressure, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in Pressure.csv: "+pressureDataset.count())

    var humidityDataset = filterAndModify(readFile(humidity, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in Pressure.csv: "+humidityDataset.count())

    var temperatureDataset = filterAndModify(readFile(temperature, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in temperature.csv: "+temperatureDataset.count())

    var weather_descriptionDataset = filterAndModify(readFile(weather_description, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in weather_description.csv: "+weather_descriptionDataset.count())

    var wind_directionDataset = filterAndModify(readFile(wind_direction, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in wind_direction.csv: "+wind_directionDataset.count())

    var wind_speedDataset = filterAndModify(readFile(pressure, sparkSession.read.option("header", true).schema(dataSchema(StringType)).option("mode", "DROPMALFORMED")))
    print("Total Rows in wind_speed.csv: "+wind_speedDataset.count())

    val mapIm =
      Map("pressureDataset" -> pressureDataset,
        "humidityDataset" -> humidityDataset,
        "temperatureDataset" -> temperatureDataset,
        "wind_directionDataset" -> wind_directionDataset,
        "wind_speedDataset" -> wind_speedDataset
      )
    
    val ignoreCols = List("Month", "Year", "DayBucket", "Daily")
    for((datasetType, datasetValue) <- mapIm) {
      for(timeColumn <- ignoreCols) {
        for(column <- datasetValue.columns) {
          if(!ignoreCols.contains(column)) {
            segmentBucket(datasetValue, timeColumn, column, datasetType)
          }
        }
      }
    }

    for(timeColumn <- ignoreCols) {
      for(column <- weather_descriptionDataset.columns) {
        if(!ignoreCols.contains(column)) {
          segmentCategoricalBucket(weather_descriptionDataset, timeColumn, column, "weather_descriptionDataset")
        }
      }
    }
  }

  def segmentCategoricalBucket(dataset: Dataset[Row], timeColumn: String, dataColumn: String, datasetType: String)= {
    var  modifiedDataset = dataset
      .select(timeColumn, dataColumn)
      .withColumn("data", UDFUtils.valueToString(dataset(dataColumn)))
      .filter("data != 'NA'")
      .drop(dataColumn);

    modifiedDataset = modifiedDataset.groupBy(timeColumn)
      .agg(
        functions.collect_list("data").as("All"),
        functions.count("data").as("Total"))

    modifiedDataset = modifiedDataset
      .withColumn("MaxPercentage", UDFUtils.toPercentage(modifiedDataset("All"), modifiedDataset("Total"))).drop("All")
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("/tmp/solution1/" + dataColumn + "/" + datasetType + "/" +timeColumn + "/")
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("s3a://solution1/"+ dataColumn + "/" + datasetType + "/" +timeColumn)
  }

  def segmentBucket(dataset: Dataset[Row], timeColumn: String, dataColumn: String, datasetType: String)= {
    val folderName = timeColumn + "-" + dataColumn;
    var  modifiedDataset = dataset
      .select(timeColumn, dataColumn)
      .withColumn("data", UDFUtils.toFloat(dataset(dataColumn)))
      .filter("data != 11111")
      .drop(dataColumn);
    modifiedDataset = modifiedDataset.groupBy(timeColumn)
      .agg(
        functions.avg("data").as("Avg"),
        functions.count("data").as("Total"),
        functions.max("data").as("Max"),
        functions.min("data").as("Min"))
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("/tmp/solution1/" + dataColumn + "/" + datasetType + "/" +timeColumn + "/")
    modifiedDataset.coalesce(1).write.format("json").mode("overwrite").save("s3a://solution1/"+ dataColumn + "/" + datasetType + "/" +timeColumn)
  }


  def filterAndModify(dataset: Dataset[Row]) = {
    val modifiedDataset = dataset.withColumn("Month", UDFUtils.toMonth(dataset("datetime")))
      .withColumn("Year", UDFUtils.toYear(dataset("datetime")))
      .withColumn("Hour", UDFUtils.toHour(dataset("datetime")))
      .withColumn("Daily", UDFUtils.toDay(dataset("datetime")))
      .withColumn("DayBucket", UDFUtils.toTimeBucket(dataset("datetime"))).drop("datetime")
    modifiedDataset
  }

  /**
    * Map between State and City
    */

  def createCityMap(dataset: Dataset[Row]): Map[String, String] = {
    val cityCountryMap = dataset.select("City", "Country").collect().map(r => (r.get(0).toString, r.get(1).toString)).toMap
    print(cityCountryMap)
    cityCountryMap
  }

  def getSparkSession(appName: String, master: String) = {
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master).getOrCreate
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

  def dataSchema(colType: DataType) = {
    StructType(Array(
      StructField("datetime", TimestampType, true), StructField("Vancouver", colType, true), StructField("Portland", colType, true), StructField("San Francisco", colType, true), StructField("Seattle", colType, true), StructField("Los Angeles", colType, true), StructField("San Diego", colType, true), StructField("Las Vegas", colType, true), StructField("Phoenix", colType, true), StructField("Albuquerque", colType, true), StructField("Denver", colType, true), StructField("San Antonio", colType, true), StructField("Dallas", colType, true), StructField("Houston", colType, true), StructField("Kansas City", colType, true), StructField("Minneapolis", colType, true), StructField("Saint Louis", colType, true), StructField("Chicago", colType, true), StructField("Nashville", colType, true), StructField("Indianapolis", colType, true), StructField("Atlanta", colType, true), StructField("Detroit", colType, true), StructField("Jacksonville", colType, true), StructField("Charlotte", colType, true), StructField("Miami", colType, true), StructField("Pittsburgh", colType, true), StructField("Toronto", colType, true), StructField("Philadelphia", colType, true), StructField("New York", colType, true), StructField("Montreal", colType, true), StructField("Boston", colType, true), StructField("Beersheba", colType, true), StructField("Tel Aviv District", colType, true), StructField("Eilat", colType, true), StructField("Haifa", colType, true), StructField("Nahariyya", colType, true), StructField("Jerusalem", colType, true)
    ))
  }

  def citySchema() = {
    StructType(Array(
      StructField("City", StringType, true),
      StructField("Country", StringType, true),
      StructField("Latitude", FloatType, true),
      StructField("Longitude", FloatType, true)))
  }

  def readWithHeader(schema: StructType, sparkSession: SparkSession) = {
    sparkSession.read.option("header", true).schema(schema).option("mode", "DROPMALFORMED")
  }
}
