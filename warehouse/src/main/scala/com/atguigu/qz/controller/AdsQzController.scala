package com.atguigu.qz.controller

import com.atguigu.qz.service.AdsQzService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description: 报表层指标统计Controller
  * @Author: my.yang
  * @Date: 2019/9/1 1:17 PM
  */
object AdsQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName("AdsQzController").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // ads层表建表时未启用压缩所以以下配置暂时关闭
    //    // 开启压缩
    //    HiveUtil.openCompression(sparkSession)
    //    // 使用Snappy压缩
    //    HiveUtil.useSnappyCompression(sparkSession)
    // 声明日期
    var dt = "20190722"
    // 执行ADS指标计算逻辑
    AdsQzService.getTarget(sparkSession, dt)
    AdsQzService.getTargetApi(sparkSession, dt)
  }
}
