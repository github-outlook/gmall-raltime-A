package com.github_zhu.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/15 21:12
 * @ModifiedBy:
 *
 */
object MyKafkaSink{

  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val brokerList: String = properties.getProperty("kafka.broker.list")

  var kafkaProducer : KafkaProducer[String,String] = null

  def createKafkaProducer :KafkaProducer[String,String]={
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers",brokerList)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("enable.idompotence",(true: java.lang.Boolean))

    var producer: KafkaProducer[String, String] = null


      producer = new KafkaProducer[String, String](properties)



    producer
  }
  def send(topic :String,msg:String):Unit={
    if(kafkaProducer==null) kafkaProducer= createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String,String](topic,msg))
  }
  def send(topic :String,key:String ,msg:String):Unit={
    if(kafkaProducer==null) kafkaProducer= createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String,String](topic,key,msg))
  }


}
