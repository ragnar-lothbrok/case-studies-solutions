package com.simplilearn.bigdata.january_casestudy_1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      print("Please provide <kafka_host> <topic> <filename>")
      System.exit(0)
    }
    val filePath:String = args(2)
    val topic = args(1)
    val kafkaHost = args(0)
    val producer = getKafkaProducer(topic, kafkaHost)
    println("Started.")
    for (line <- Source.fromFile(filePath).getLines) {
      val record = new ProducerRecord[String, String](topic, null, line)
      producer.send(record).get();
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

