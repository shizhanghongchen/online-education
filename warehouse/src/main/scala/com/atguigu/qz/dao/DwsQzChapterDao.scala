package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws章节维度宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 2:59 PM
  */
object DwsQzChapterDao {

  /**
    * 查询dwd.qz_chapter基础数据 ---> 章节数据表
    *
    * @param sparkSession
    * @return
    */
  def getDwdQzChapter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select chapterid,chapterlistid,chaptername,sequence,showstatus,status,creator as " +
      "chapter_creator,createtime as chapter_createtime,courseid as chapter_courseid,chapternum,outchapterid,dt,dn from dwd.dwd_qz_chapter where " +
      s"dt='$dt'")
  }

  /**
    * 查询dwd.qz_chapter_list基础数据 ---> 章节列表
    *
    * @param sparkSession
    * @param dt
    */
  def getDwdQzChapterList(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select chapterlistid,chapterlistname,chapterallnum,dn from dwd.dwd_qz_chapter_list " +
      s"where dt='$dt'")
  }

  /**
    * 查询dwd.qz_point基础数据 ---> 知识点数据日志表
    *
    * @param sparkSession
    * @param dt
    */
  def getDwdQzPoint(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select pointid,pointname,pointyear,chapter,excisenum,pointlistid,chapterid," +
      "pointdescribe,pointlevel,typelist,score as point_score,thought,remid,pointnamelist,typelistids,pointlist,dn from " +
      s"dwd.dwd_qz_point where dt='$dt'")
  }

  /**
    * 查询dwd.qz_point_question基础数据 ---> 做题知识点关联数据表
    *
    * @param sparkSession
    * @param dt
    */
  def getDwdQzPointQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select pointid,questionid,questype,dn from dwd.dwd_qz_point_question where dt='$dt'")
  }
}
