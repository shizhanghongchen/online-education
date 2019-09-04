package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws用户试卷详情宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 4:18 PM
  */
object DwsUserPaperDetailDao {

  /**
    * 查询dwd.dwd_qz_member_paper_question基础数据 ---> 学员做题详情数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwdQzMemberPaperQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select userid,paperviewid,chapterid,sitecourseid,questionid,majorid,useranswer,istrue,lasttime,opertype," +
      s"paperid,spendtime,score,question_answer,dt,dn from dwd.dwd_qz_member_paper_question where dt='$dt'")
  }

  /**
    * 查询dws.dws_qz_chapter基础数据 ---> 章节维度表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwsQzChapter(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select chapterid,chapterlistid,chaptername,sequence as chapter_sequence,status as chapter_status," +
      "chapter_courseid,chapternum,chapterallnum,outchapterid,chapterlistname,pointid,questype,pointname,pointyear" +
      ",chapter,excisenum,pointlistid,pointdescribe,pointlevel,typelist,point_score,thought,remid,pointnamelist," +
      s"typelistids,pointlist,dn from dws.dws_qz_chapter where dt='$dt'")
  }

  /**
    * 查询dws.dws_qz_course基础数据 ---> 课程维度表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwsQzCourse(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select sitecourseid,siteid,courseid,sitecoursename,coursechapter,sequence as course_sequence," +
      "status as course_status,sitecourse_creator,sitecourse_createtime,helppaperstatus,servertype,boardid,showstatus,majorid," +
      s"coursename,isadvc,chapterlistid,pointlistid,courseeduid,edusubjectid,dn from dws.dws_qz_course where dt='$dt'")
  }

  /**
    * 查询dws.dws_qz_major基础数据 ---> 主修维度表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select majorid,businessid,majorname,shortname,status as major_status,sequence  as major_sequence," +
      "major_creator,major_createtime,businessname,sitename,domain,multicastserver,templateserver,multicastgateway,multicastport," +
      s"dn from dws.dws_qz_major where dt=$dt")
  }

  /**
    * 查询dws.dws_qz_paper基础数据 ---> 试卷维度表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwsQzPaper(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,iscontest,contesttime," +
      "conteststarttime,contestendtime,contesttimelimit,dayiid,status as paper_status,paper_view_creator,paper_view_createtime," +
      "paperviewcatid,modifystatus,description,paperuse,testreport,centerid,sequence as paper_sequence,centername,centeryear," +
      "centertype,provideuser,centerviewtype,stage as paper_stage,papercatid,courseid,paperyear,suitnum,papername,totalscore,dn" +
      s" from dws.dws_qz_paper where dt=$dt")
  }

  /**
    * 查询dws.dws_qz_question基础数据 ---> 题目维度表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getDwsQzQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select questionid,parentid as question_parentid,questypeid,quesviewtype,content as question_content," +
      "answer as question_answer,analysis as question_analysis,limitminute as question_limitminute,score as question_score," +
      "splitscore,lecture,creator as question_creator,createtime as question_createtime,modifystatus as question_modifystatus," +
      "attanswer as question_attanswer,questag as question_questag,vanalysisaddr as question_vanalysisaddr,difficulty as question_difficulty," +
      "quesskill,vdeoaddr,description as question_description,splitscoretype as question_splitscoretype,dn " +
      s" from dws.dws_qz_question where dt=$dt")
  }
}
