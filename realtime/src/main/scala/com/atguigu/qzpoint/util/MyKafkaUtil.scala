package com.atguigu.qzpoint.util

import java.lang
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @description: Kafka工具类
  * @Author: my.yang
  * @Date: 2019/9/3 8:13 PM
  */
object MyKafkaUtil {

  /**
    * 获取配置文件工具类
    */
  private val properties: Properties = PropertiesUtil.load("config.properties")

  /**
    * 获取KafkaMap
    *
    * @return
    */
  def getKafkaMap(groupId: String): Map[String, Object] = {
    // kafka消费者配置
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> properties.getProperty("auto.offset.reset.earliest"),
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    kafkaMap
  }

  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题

}
