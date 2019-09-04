package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws课程维度宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 3:22 PM
  */
object DwsQzCourseDao {

  /**
    * 查询dwd.dwd_qz_site_course基础数据 ---> 网站课程日志数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzSiteCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence,status," +
      "creator as sitecourse_creator,createtime as sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,dt,dn " +
      s"from dwd.dwd_qz_site_course where dt='${dt}'")
  }

  /**
    * 查询dwd.dwd_qz_course基础数据 ---> 题库课程数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from " +
      s"dwd.dwd_qz_course where dt='${dt}'")
  }

  /**
    * 查询dwd.dwd_qz_course_edusubject基础数据  ---> 课程辅导数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzCourseEduSubject(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select courseeduid,edusubjectid,courseid,dn from dwd.dwd_qz_course_edusubject " +
      s"where dt='${dt}'")
  }
}
