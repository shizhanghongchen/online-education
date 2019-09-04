package com.atguigu.qz.controller

import com.atguigu.qz.service.EtlDataService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description: 解析数据 QZ导入dwd层
  * @Author: my.yang
  * @Date: 2019/8/31 10:07 AM
  */
object DwdQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("DwdController").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    HiveUtil.openCompression(sparkSession)
    // 使用Snappy压缩
    HiveUtil.useSnappyCompression(sparkSession)
    // 执行导入逻辑
    EtlDataService.etlQzChapter(ssc, sparkSession)
    EtlDataService.etlQzChapterList(ssc, sparkSession)
    EtlDataService.etlQzPoint(ssc, sparkSession)
    EtlDataService.etlQzPointQuestion(ssc, sparkSession)
    EtlDataService.etlQzSiteCourse(ssc, sparkSession)
    EtlDataService.etlQzCourse(ssc, sparkSession)
    EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
    EtlDataService.etlQzWebsite(ssc, sparkSession)
    EtlDataService.etlQzMajor(ssc, sparkSession)
    EtlDataService.etlQzBusiness(ssc, sparkSession)
    EtlDataService.etlQzPaperView(ssc, sparkSession)
    EtlDataService.etlQzCenterPaper(ssc, sparkSession)
    EtlDataService.etlQzPaper(ssc, sparkSession)
    EtlDataService.etlQzCenter(ssc, sparkSession)
    EtlDataService.etlQzQuestion(ssc, sparkSession)
    EtlDataService.etlQzQuestionType(ssc, sparkSession)
    EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)
  }
}
