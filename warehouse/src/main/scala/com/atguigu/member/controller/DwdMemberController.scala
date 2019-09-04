package com.atguigu.member.controller

import com.atguigu.member.service.EtlDataService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description: dwd controller
  * @Author: my.yang
  * @Date: 2019/8/30 7:33 PM
  */
object DwdMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    HiveUtil.openCompression(sparkSession)
    // 使用snappy压缩
    HiveUtil.useSnappyCompression(sparkSession)
    // 对用户原始数据进行数据清洗 存入bdl层表中
    // 导入基础广告表数据
    EtlDataService.etlBaseAdLog(ssc, sparkSession)
    // 导入基础网站表数据
    EtlDataService.etlBaseWebSiteLog(ssc, sparkSession)
    // 清洗用户数据
    EtlDataService.etlMemberLog(ssc, sparkSession)
    // 清洗用户注册数据
    EtlDataService.etlMemberRegtypeLog(ssc, sparkSession)
    // 导入用户支付情况记录
    EtlDataService.etlMemPayMoneyLog(ssc, sparkSession)
    // 导入vip基础数据
    EtlDataService.etlMemVipLevelLog(ssc, sparkSession)
  }
}
