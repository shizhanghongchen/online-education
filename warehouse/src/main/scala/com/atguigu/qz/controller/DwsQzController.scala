package com.atguigu.qz.controller

import com.atguigu.qz.service.DwsQzService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @description:   基于dwd层基础表数据对表进行维度退化进行表聚合;
  *                 1. dws.dws_qz_chapter(章节维度表);
  *                 2. dws.dws_qz_course（课程维度表);
  *                 3. dws.dws_qz_major(主修维度表);
  *                 4. dws.dws_qz_paper(试卷维度表);
  *                 5. dws.dws_qz_question(题目维度表);
  * @Author: my.yang
  * @Date: 2019/8/31 11:59 AM
  */
object DwsQzController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName("DwsQzController").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    HiveUtil.openCompression(sparkSession)
    // 使用Snappy压缩
    HiveUtil.useSnappyCompression(sparkSession)
    // 定义时间
    val dt = "20190722"
    // 执行生成宽表逻辑
    DwsQzService.saveDwsQzChapter(sparkSession, dt)
    DwsQzService.saveDwsQzCourse(sparkSession, dt)
    DwsQzService.saveDwsQzMajor(sparkSession, dt)
    DwsQzService.saveDwsQzPaper(sparkSession, dt)
    DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
    DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)
  }
}
