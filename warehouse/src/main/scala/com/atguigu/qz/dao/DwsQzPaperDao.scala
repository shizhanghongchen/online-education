package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws试卷维度宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 3:43 PM
  */
object DwsQzPaperDao {

  /**
    * 查询dwd.dwd_qz_paper_view基础数据 ---> 试卷视图数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzPaperView(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest," +
      "contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,creator as paper_view_creator," +
      "createtime as paper_view_createtime,paperviewcatid,modifystatus,description,papertype,downurl,paperuse," +
      s"paperdifficult,testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_center_paper基础数据 ---> 试卷主题关联数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzCenterPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select paperviewid,sequence,centerid,dn from dwd.dwd_qz_center_paper where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_paper基础数据 ---> 做题试卷日志数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperid,papercatid,courseid,paperyear,chapter,suitnum,papername,totalscore,chapterid," +
      s"chapterlistid,dn from dwd.dwd_qz_paper where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_center基础数据 ---> 主题数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzCenter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select centerid,centername,centeryear,centertype,centerparam,provideuser," +
      s"centerviewtype,stage,dn from dwd.dwd_qz_center where dt='$dt'")
  }
}
