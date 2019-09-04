package com.atguigu.qz.service

import com.atguigu.qz.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @description: 报表层指标统计服务类
  * @Author: my.yang
  * @Date: 2019/9/1 1:24 PM
  */
object AdsQzService {

  /**
    * 报表层指标统计(Spark Sql)方式实现
    *
    * @param sparkSession
    * @param dt
    */
  def getTarget(sparkSession: SparkSession, dt: String): Unit = {
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷平均耗时 平均分指标
    val avgDetail = AdsQzDao.getAvgSPendTimeAndScore(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计试卷 最高分 最低分指标
    val topscore = AdsQzDao.getTopScore(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数前三用户详情
    val top3UserDetail = AdsQzDao.getTop3UserDetail(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数倒数三的用户详情
    val low3UserDetail = AdsQzDao.getLow3UserDetail(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷 各分段学员名称
    val paperScore = AdsQzDao.getPaperScoreSegmentUser(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷未及格人数 及格人数 及格率
    val paperPassDetail = AdsQzDao.getPaperPassDetail(sparkSession, dt)
    // 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid
    val questionDetail = AdsQzDao.getQuestionDetail(sparkSession, dt)
  }

  /**
    * 报表层指标统计(Spark DataFrame Api)方式实现
    *
    * @param sparkSession
    * @param dt
    */
  def getTargetApi(sparkSession: SparkSession, dt: String): Unit = {

    // 隐式转换
    import org.apache.spark.sql.functions._

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷平均耗时 平均分指标;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_paper_avgtimeandscore;
      */
    val avgDetail = sparkSession.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail ")
      .where(s"dt=${dt}").groupBy("paperviewid", "paperviewname", "dt", "dn").
      agg(avg("score").cast("decimal(4,1)").as("avgscore"),
        avg("spendtime").cast("decimal(10,1)").as("avgspendtime"))
      .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计试卷 最高分 最低分指标;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_paper_maxdetail;
      */
    val topscore = sparkSession.sql("select paperviewid,paperviewname,score,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=$dt").groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(max("score").as("maxscore"), min("score").as("minscore"))
      .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数前三用户详情;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_top3_userdetail;
      */
    val top3UserDetail = sparkSession.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt=$dt").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
      , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,按试卷分组获取每份试卷的分数倒数三的用户详情;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_low3_userdetail;
      */
    val low3UserDetail = sparkSession.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt=$dt").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
      , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷 各分段学员名称;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_paper_scoresegment_user;
      */
    val paperScore = sparkSession.sql("select *from dws.dws_user_paper_detail")
      .where(s"dt=$dt")
      .select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
      .withColumn("score_segment",
        when(col("score").between(0, 20), "0-20")
          .when(col("score") > 20 && col("score") <= 40, "20-40")
          .when(col("score") > 40 && col("score") <= 60, "40-60")
          .when(col("score") > 60 && col("score") <= 80, "60-80")
          .when(col("score") > 80 && col("score") <= 100, "80-100"))
      .drop("score").groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
      .agg(concat_ws(",", collect_list(col("userid").cast("string").as("userids"))).as("userids"))
      .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .orderBy("paperviewid", "score_segment")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")

    /**
      * 1. 增加dws.dws_user_paper_detail用户试卷详情宽表数据缓存以优化程序执行效率;
      * 2. 多个引用之间共享此数据;
      */
    val paperPassDetail = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷未及格人数;
      */
    val unPassDetail = paperPassDetail.select("paperviewid", "paperviewname", "dn", "dt")
      .where(s"dt='$dt'").where("score between 0 and 60")
      .groupBy("paperviewid", "paperviewname", "dn", "dt")
      .agg(count("paperviewid").as("unpasscount"))

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷及格人数;
      */
    val passDetail = paperPassDetail.select("paperviewid", "dn")
      .where(s"dt='$dt'").where("score >60")
      .groupBy("paperviewid", "dn")
      .agg(count("paperviewid").as("passcount"))

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各试卷及格率;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_user_paper_detai;
      */
    unPassDetail.join(passDetail, Seq("paperviewid", "dn")).
      withColumn("rate", (col("passcount")./(col("passcount") + col("unpasscount")))
        .cast("decimal(4,2)"))
      .select("paperviewid", "paperviewname", "unpasscount", "passcount", "rate", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

    /**
      * 删除缓存释放空间
      */
    paperPassDetail.unpersist()

    /**
      * 1. 增加dws.dws_user_paper_detail用户试卷详情宽表数据缓存以优化程序执行效率;
      * 2. 多个引用之间共享此数据;
      */
    val userQuestionDetail = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各题错误人数;
      */
    val userQuestionError = userQuestionDetail.select("questionid", "dt", "dn", "user_question_answer")
      .where(s"dt='$dt'").where("user_question_answer='0'").drop("user_question_answer")
      .groupBy("questionid", "dt", "dn")
      .agg(count("questionid").as("errcount"))

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各题正确人数;
      */
    val userQuestionRight = userQuestionDetail.select("questionid", "dn", "user_question_answer")
      .where(s"dt='$dt'").where("user_question_answer='1'").drop("user_question_answer")
      .groupBy("questionid", "dn")
      .agg(count("questionid").as("rightcount"))

    /**
      * 1. 基于dws.dws_user_paper_detail用户试卷详情宽表数据,统计各题错题率 top3错误题数多的questionid;
      * 2. 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追
      * 加的方式保存数据到hive表中 ---> ads.ads_user_question_detail;
      */
    userQuestionError.join(userQuestionRight, Seq("questionid", "dn"))
      .withColumn("rate", (col("errcount") / (col("errcount") + col("rightcount"))).cast("decimal(4,2)"))
      .orderBy(desc("errcount")).coalesce(1)
      .select("questionid", "errcount", "rightcount", "rate", "dt", "dn")
      .write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")
  }
}


























