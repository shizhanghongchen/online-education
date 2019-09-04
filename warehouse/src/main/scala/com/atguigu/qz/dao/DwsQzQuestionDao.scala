package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws题目维度宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 3:54 PM
  */
object DwsQzQuestionDao {

  /**
    * 查询dwd.dwd_qz_question基础数据 ---> 做题日志数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getQzQuestion(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select questionid,parentid,questypeid,quesviewtype,content,answer,analysis,limitminute," +
      "score,splitscore,status,optnum,lecture,creator,createtime,modifystatus,attanswer,questag,vanalysisaddr,difficulty," +
      s"quesskill,vdeoaddr,dt,dn from  dwd.dwd_qz_question where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_question_type基础数据  ---> 题目类型数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getQzQuestionType(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select questypeid,viewtypename,description,papertypename,remark,splitscoretype,dn from " +
      s"dwd.dwd_qz_question_type where dt='$dt'")
  }
}
