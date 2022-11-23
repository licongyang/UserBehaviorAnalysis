package com.example.hotitems_analysis

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object MyKafkaProducer {

  def writeTokafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个kafka producer
    // kafka的api (key:string, value:string)
    val producer = new KafkaProducer[String, String](properties)
    // 从文件中读取数据，发送
    val bufferedSource = io.Source.fromFile("/Users/alfie/workspace/code/learn/flink-lean/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      // (key:string, value:string)
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    // 处理完关闭
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeTokafka("hotItems2")
  }

}
