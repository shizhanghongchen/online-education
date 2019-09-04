package com.atguigu.qzpoint.producer

import java.util.Properties

import com.atguigu.qzpoint.util.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object QzLogProducer {
  def main(args: Array[String]): Unit = {
    /**
      * 获取配置文件工具类
      */
    val properties = PropertiesUtil.load("config.properties")
    val sparkConf = new SparkConf().setAppName("QzLogProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    System.setProperty("HADOOP_USER_NAME", properties.getProperty("hadoop.user.name"))
    val resultLog = ssc.textFile(this.getClass.getResource("/qz.log").toURI.getPath, 10)
      .foreachPartition(partitoin => {
        val props = new Properties()
        props.put("bootstrap.servers", properties.getProperty("kafka.broker.list"))
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partitoin.foreach(item => {
          val msg = new ProducerRecord[String, String](properties.getProperty("qz.log.topic"), item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
