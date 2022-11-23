package com.example.hotitems_analysis

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util._

import scala.collection.JavaConverters._


object MyKafkaConsumer {

  def readFromKafka(topic: String): Unit = {
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    //序列化
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 没配checkpoint,偏移量重置，选latest
    properties.setProperty("auto.offset.reset", "earliest")
    //    properties.setProperty("auto.offset.reset", "latest")

    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(java.util.Collections.singletonList(topic))

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        println(record)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    readFromKafka("hotItems2")
  }

}
