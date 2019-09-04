package com.atguigu.member.controller

import com.atguigu.member.service.AdsMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description: ads controller
  * @Author: my.yang
  * @Date: 2019/8/30 8:37 PM
  */
object AdsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("AdsMemberController").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    AdsMemberService.queryDetailApi(sparkSession, "20190722")
    AdsMemberService.queryDetailSql(sparkSession, "20190722")
  }
}
