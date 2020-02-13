package com.simplilearn.bigdata.january_casestudy_3

import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

case class Channel(
                    channelId: String,
                    title: String,
                    assignable: Boolean
                  )

case class Category(
                     kind: String,
                     etag: String,
                     id: String,
                     snippet: Channel
                   )


object KafkaProducer {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      print("Please provide <kafka_host> <topic> <filename>")
      System.exit(0)
    }
    val filePath: String = args(2)
    val topic = args(1)
    val kafkaHost = args(0)
    val producer = getKafkaProducer(topic, kafkaHost)
    println("Started.")
    val mapper = new ObjectMapper()
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    val categories:JsonNode  = mapper.readTree(lines)
    val iterator =  categories.elements()
    while(iterator.hasNext) {
      val node  = iterator.next()
      val categoryId = node.get("id").asText()
      val title  = node.get("snippet").get("title").asText()
      val record = categoryId + "," + title
      println(record)
      producer.send(new ProducerRecord[String, String](topic, null, record)).get();
    }
    println("Finished.")
  }

  def getKafkaProducer(topic: String, kafkaHost: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaHost)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

}

