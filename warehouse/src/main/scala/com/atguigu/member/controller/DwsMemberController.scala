package com.atguigu.member.controller

import com.atguigu.member.bean.DwsMember
import com.atguigu.member.service.DwsMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description: dws controller
  * @Author: my.yang
  * @Date: 2019/8/30 8:15 PM
  */
object DwsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("DwsMemberController")
      .setMaster("local[*]")
    //          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //          .registerKryoClasses(Array(classOf[DwsMember]))
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    HiveUtil.openCompression(sparkSession)
    // 使用snappy压缩
    HiveUtil.useSnappyCompression(sparkSession)
    // 根据用户信息聚合用户表数据
//    DwsMemberService.importMember(sparkSession, "20190722")
    DwsMemberService.importMemberUseApi(sparkSession, "20190722")
  }
}
