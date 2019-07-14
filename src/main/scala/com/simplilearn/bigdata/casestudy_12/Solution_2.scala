package com.simplilearn.bigdata.casestudy_12

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, StreamingContext}

object Solution_2 {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.out.println("Please provide <broker_url> <topic_name> <group_id> <spark_master> <duration_in_minutes_streaming_window> <Top X values>")
      System.exit(0)
    }
    val brokerUrl = args(0)
    val topicName = args(1)
    val groupId = args(2)
    val sparkMaster = args(3)
    val durationInMinutes = args(4).toInt
    val N = args(5).toInt
    val sparkConf = new SparkConf
    sparkConf.setAppName("Spark Streaming App - Bottom " + N + " trending brand in last " + durationInMinutes + " minutes")
    sparkConf.setMaster(if (sparkMaster.indexOf("local") != -1) "local[*]"
    else sparkMaster)

    val streamingContext = new StreamingContext(sparkConf, Minutes(durationInMinutes))
    val kafkaParams = collection.mutable.Map[String, Object]()
    kafkaParams.put("bootstrap.servers", brokerUrl)
    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id", groupId)
    kafkaParams.put("auto.offset.reset", "latest")
    kafkaParams.put("enable.auto.commit", "false")
    val topics = Array(topicName)

    val kafkaStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParams))
    val allProducts = kafkaStream.map((record) => (record.value))

    val brandOrderCount = allProducts.map((record) => (record.toString.split(",")(2), "1".toInt)).reduceByKey((i1, i2) => i1 + i2)
    val topN = brandOrderCount.transform((rdd) => {
      rdd.sortBy(_._2, true)
    })
    topN.print(N)
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
