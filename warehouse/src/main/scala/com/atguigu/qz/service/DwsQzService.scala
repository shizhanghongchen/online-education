package com.atguigu.qz.service

import com.atguigu.qz.dao.{DwsUserPaperDetailDao, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @description: 基于dwd层基础表数据对表进行维度退化以及进行表聚合 ;
  *               1. dws.dws_qz_chapter (章节维度表);
  *               2. dws.dws_qz_course（课程维度表);
  *               3. dws.dws_qz_major (主修维度表);
  *               4. dws.dws_qz_paper (试卷维度表);
  *               5. dws.dws_qz_question (题目维度表);
  *               6. dw.user_paper_detail (用户试卷详情宽表);
  * @Author: my.yang
  * @Date: 2019/8/31 2:46 PM
  */
object DwsQzService {

  /**
    * 聚合生成dws.dws_qz_chapter (章节维度表)
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzChapter(sparkSession: SparkSession, dt: String): Unit = {
    // 获取章节表数据
    val dwdQzChapter = DwsQzChapterDao.getDwdQzChapter(sparkSession, dt)
    // 获取章节列表数据
    val dwdQzChapterList = DwsQzChapterDao.getDwdQzChapterList(sparkSession, dt)
    // 获取知识点日志数据
    val dwdQzPoint = DwsQzChapterDao.getDwdQzPoint(sparkSession, dt)
    // 获取做题知识点关联数据
    val dwdQzPointQuestion = DwsQzChapterDao.getDwdQzPointQuestion(sparkSession, dt)

    /**
      * Join生成章节维度宽表 ---> 主表 : 章节表
      *   1. dwd.dwd_qz_chapter  inner join  dwd.qz_chapter_list,  join条件 : chapterlistid 和 dn;
      *   2. inner join dwd.dwd_qz_point, join条件 : chapterid 和 dn;
      *   3. inner join dwd.dwd_qz_point_question,  join条件 : pointid 和 dn;
      */
    val result: DataFrame = dwdQzChapter.join(dwdQzChapterList, Seq("chapterlistid", "dn"))
      .join(dwdQzPoint, Seq("chapterid", "dn"))
      .join(dwdQzPointQuestion, Seq("pointid", "dn"))
      .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status",
        "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
        "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
        "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
    // 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
  }

  /**
    * 聚合生成dws.dws_qz_course（课程维度表)
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzCourse(sparkSession: SparkSession, dt: String): Unit = {
    // 获取网站课程日志数据
    val dwdQzSiteCourse = DwsQzCourseDao.getDwdQzSiteCourse(sparkSession, dt)
    // 获取题库课程数据
    val dwdQzCourse = DwsQzCourseDao.getDwdQzCourse(sparkSession, dt)
    // 获取课程辅导数据
    val dwdQzCourseEdusubject = DwsQzCourseDao.getDwdQzCourseEduSubject(sparkSession, dt)

    /**
      * Join生成课程维度宽表 ---> 主表 : 网站课程日志数据表
      *   1. dwd.dwd_qz_site_course inner join dwd.qz_course, join条件 : courseid 和 dn;
      *   2. inner join dwd.qz_course_edusubject, join条件 : courseid 和 dn;
      */
    val result = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEdusubject, Seq("courseid", "dn"))
      .select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
        "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
        "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
        , "dt", "dn")
    // 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }

  /**
    * 聚合生成dws.dws_qz_major（主修维度表)
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    // 获取主修数据
    val dwdQzMajor = DwsQzMajorDao.getQzMajor(sparkSession, dt)
    // 获取做题网站日志数据
    val dwdQzWebsite = DwsQzMajorDao.getQzWebsite(sparkSession, dt)
    // 获取所属行业数据
    val dwdQzBusiness = DwsQzMajorDao.getQzBusiness(sparkSession, dt)

    /**
      * Join生成主修维度宽表 ---> 主表 : 主修数据表
      *   1. dwd.dwd_qz_major inner join dwd.dwd_qz_website, join条件 : siteid 和 dn;
      *   2. inner join dwd.dwd_qz_business, join条件 : businessid 和 dn;
      */
    val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    // 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  /**
    * 聚合生成dws.dws_qz_paper (试卷维度表)
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzPaper(sparkSession: SparkSession, dt: String): Unit = {
    // 获取试卷视图数据
    val dwdQzPaperView = DwsQzPaperDao.getDwdQzPaperView(sparkSession, dt)
    // 获取试卷主题关联数据
    val dwdQzCenterPaper = DwsQzPaperDao.getDwdQzCenterPaper(sparkSession, dt)
    // 获取主题数据
    val dwdQzCenter = DwsQzPaperDao.getDwdQzCenter(sparkSession, dt)
    // 获取做题试卷日志数据
    val dwdQzPaper = DwsQzPaperDao.getDwdQzPaper(sparkSession, dt)

    /**
      * Join生成主修维度宽表 ---> 主表 : 试卷视图数据表
      *  1. qz_paperview left join qz_center, join条件 : paperviewid 和 dn;
      *  2. left join qz_center, join条件 : centerid 和 dn;
      *  3. inner join qz_paper, join条件 : paperid 和 dn;
      */
    val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn")
    // 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }

  /**
    * 聚合生成dws.dws_qz_question (题目维度表)
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String): Unit = {
    // 获取做题日志数据
    val dwdQzQuestion = DwsQzQuestionDao.getQzQuestion(sparkSession, dt)
    // 获取题目类型数据
    val dwdQzQuestionType = DwsQzQuestionDao.getQzQuestionType(sparkSession, dt)

    /**
      * Join生成题目维度宽表 ---> 主表 : 做题日志数据表
      *  1. qz_quesiton inner join qz_questiontype, join条件 : questypeid 和 dn;
      */
    val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    // 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
  }

  /**
    * 聚合生成dw.user_paper_detail (用户试卷详情宽表)
    *
    * @param sparkSession
    * @param dt
    */
  def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String): Unit = {
    // 获取学员做题详情数据 : 删除paperid字段同时将question_answer字段重命名为user_question_answer
    val dwdQzMemberPaperQuestion = DwsUserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
      .withColumnRenamed("question_answer", "user_question_answer")
    // 获取章节维度数据 : 删除courseid字段
    val dwsQzChapter = DwsUserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
    // 获取课程维度数据 : sitecourse_creator重命名为course_creator,sitecourse_createtime重命名为course_createtime,删除majorid,chapterlistid,pointlistid字段
    val dwsQzCourse = DwsUserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    // 获取主修维度数据
    val dwsQzMajor = DwsUserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
    // 获取试卷维度数据 : 删除courseid字段
    val dwsQzPaper = DwsUserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
    // 获取题目维度数据
    val dwsQzQuestion = DwsUserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)

    /**
      * Join生成用户试卷详情宽表 ---> 主表 : 学员做题详情数据表,
      * 获取数据以DataFrame形式手动减少DataFrame的partition数量且不触发shuffle,并且以追加的方式保存数据到hive表中
      *  1. dwd_qz_member_paper_question inner join dws_qz_chapter, join条件 : chapterid 和 dn;
      *  2. inner join dws_qz_course, join条件 : sitecourseid 和 dn;
      *  3. inner join dws_qz_major, join条件 : majorid 和 dn;
      *  4. inner join dws_qz_paper, join条件 : paperviewid 和 dn;
      *  5. inner join dws_qz_question, join条件 : questionid 和 dn;
      */
    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn"))
      .join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
  }
}




















