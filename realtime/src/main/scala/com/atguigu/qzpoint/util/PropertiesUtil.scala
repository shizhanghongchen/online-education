package com.atguigu.qzpoint.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * @description: 配置文件读取工具类
  * @Author: my.yang
  * @Date: 2019/9/3 8:13 PM
  */
object PropertiesUtil {

  /**
    * 自测
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  /**
    * 配置文件加载
    *
    * @param propertieName
    * @return
    */
  def load(propertieName: String): Properties = {
    val prop = new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
