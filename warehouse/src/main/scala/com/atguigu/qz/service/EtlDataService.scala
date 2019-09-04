package com.atguigu.qz.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.qz.bean.{DwdQzPaperView, DwdQzPoint, DwdQzQuestion}
import com.atguigu.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @description: QZ ETL逻辑类
  * @Author: my.yang
  * @Date: 2019/8/31 10:08 AM
  */
object EtlDataService {

  /**
    * 解析章节数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzChapter(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzChapter.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val chapterid: Int = jsonObject.getIntValue("chapterid")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val chaptername = jsonObject.getString("chaptername")
        val sequence = jsonObject.getString("sequence")
        val showstatus = jsonObject.getString("showstatus")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val courseid = jsonObject.getIntValue("courseid")
        val chapternum = jsonObject.getIntValue("chapternum")
        val outchapterid = jsonObject.getIntValue("outchapterid")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (chapterid, chapterlistid, chaptername, sequence, showstatus, status, creator, createtime, courseid, chapternum, outchapterid, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter")
  }

  /**
    * 解析章节列表数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzChapterList(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzChapterList.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val chapterlistname = jsonObject.getString("chapterlistname")
        val courseid = jsonObject.getIntValue("courseid")
        val chapterallnum = jsonObject.getIntValue("chapterallnum")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter_list")
  }

  /**
    * 解析知识点数据日志
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzPoint(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPoint.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val pointid = jsonObject.getIntValue("pointid")
        val courseid = jsonObject.getIntValue("courseid")
        val pointname = jsonObject.getString("pointname")
        val pointyear = jsonObject.getString("pointyear")
        val chapter = jsonObject.getString("chapter")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val status = jsonObject.getString("status")
        val modifystatus = jsonObject.getString("modifystatus")
        val excisenum = jsonObject.getIntValue("excisenum")
        val pointlistid = jsonObject.getIntValue("pointlistid")
        val chapterid = jsonObject.getIntValue("chapterid")
        val sequence = jsonObject.getString("sequence")
        val pointdescribe = jsonObject.getString("pointdescribe")
        val pointlevel = jsonObject.getString("pointlevel")
        val typeslist = jsonObject.getString("typelist")
        // 保留1位小数并四舍五入
        val score = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val thought = jsonObject.getString("thought")
        val remid = jsonObject.getString("remid")
        val pointnamelist = jsonObject.getString("pointnamelist")
        val typelistids = jsonObject.getString("typelistids")
        val pointlist = jsonObject.getString("pointlist")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为知识点数据日志样例类
        DwdQzPoint(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus, excisenum, pointlistid,
          chapterid, sequence, pointdescribe, pointlevel, typeslist, score, thought, remid, pointnamelist, typelistids,
          pointlist, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point")
  }

  /**
    * 解析做题知识点关联数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzPointQuestion(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPointQuestion.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val pointid = jsonObject.getIntValue("pointid")
        val questionid = jsonObject.getIntValue("questionid")
        val questtype = jsonObject.getIntValue("questtype")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (pointid, questionid, questtype, creator, createtime, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point_question")
  }

  /**
    * 解析网站课程日志数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzSiteCourse(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzSiteCourse.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val sitecourseid = jsonObject.getIntValue("sitecourseid")
        val siteid = jsonObject.getIntValue("siteid")
        val courseid = jsonObject.getIntValue("courseid")
        val sitecoursename = jsonObject.getString("sitecoursename")
        val coursechapter = jsonObject.getString("coursechapter")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val helppaperstatus = jsonObject.getString("helppaperstatus")
        val servertype = jsonObject.getString("servertype")
        val boardid = jsonObject.getIntValue("boardid")
        val showstatus = jsonObject.getString("showstatus")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
          , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_site_course")
  }

  /**
    * 解析题库课程数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzCourse(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCourse.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val courseid = jsonObject.getIntValue("courseid")
        val majorid = jsonObject.getIntValue("majorid")
        val coursename = jsonObject.getString("coursename")
        val coursechapter = jsonObject.getString("coursechapter")
        val sequence = jsonObject.getString("sequnece")
        val isadvc = jsonObject.getString("isadvc")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val status = jsonObject.getString("status")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        val pointlistid = jsonObject.getIntValue("pointlistid")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
          , chapterlistid, pointlistid, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course")
  }

  /**
    * 解析课程辅导数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzCourseEdusubject(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCourseEduSubject.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val courseeduid = jsonObject.getIntValue("courseeduid")
        val edusubjectid = jsonObject.getIntValue("edusubjectid")
        val courseid = jsonObject.getIntValue("courseid")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val majorid = jsonObject.getIntValue("majorid")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course_edusubject")
  }

  /**
    * 解析做题网站日志数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzWebsite(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzWebsite.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val domain = jsonObject.getString("domain")
        val sequence = jsonObject.getString("sequence")
        val multicastserver = jsonObject.getString("multicastserver")
        val templateserver = jsonObject.getString("templateserver")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val multicastgateway = jsonObject.getString("multicastgateway")
        val multicastport = jsonObject.getString("multicastport")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
          multicastgateway, multicastport, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_website")
  }

  /**
    * 解析主修数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzMajor(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzMajor.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val majorid = jsonObject.getIntValue("majorid")
        val businessid = jsonObject.getIntValue("businessid")
        val siteid = jsonObject.getIntValue("siteid")
        val majorname = jsonObject.getString("majorname")
        val shortname = jsonObject.getString("shortname")
        val status = jsonObject.getString("status")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val columm_sitetype = jsonObject.getString("columm_sitetype")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_major")
  }

  /**
    * 解析所属行业数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzBusiness(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzBusiness.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val businessid = jsonObject.getIntValue("businessid")
        val businessname = jsonObject.getString("businessname")
        val sequence = jsonObject.getString("sequence")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val siteid = jsonObject.getIntValue("siteid")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_business")
  }

  /**
    * 解析试卷视图数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzPaperView(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPaperView.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val paperid = jsonObject.getIntValue("paperid")
        val paperviewname = jsonObject.getString("paperviewname")
        val paperparam = jsonObject.getString("paperparam")
        val openstatus = jsonObject.getString("openstatus")
        val explainurl = jsonObject.getString("explainurl")
        val iscontest = jsonObject.getString("iscontest")
        val contesttime = jsonObject.getString("contesttime")
        val conteststarttime = jsonObject.getString("conteststarttime")
        val contestendtime = jsonObject.getString("contestendtime")
        val contesttimelimit = jsonObject.getString("contesttimelimit")
        val dayiid = jsonObject.getIntValue("dayiid")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val paperviewcatid = jsonObject.getIntValue("paperviewcatid")
        val modifystatus = jsonObject.getString("modifystatus")
        val description = jsonObject.getString("description")
        val papertype = jsonObject.getString("papertype")
        val downurl = jsonObject.getString("downurl")
        val paperuse = jsonObject.getString("paperuse")
        val paperdifficult = jsonObject.getString("paperdifficult")
        val testreport = jsonObject.getString("testreport")
        val paperuseshow = jsonObject.getString("paperuseshow")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为试卷视图数据样例类
        DwdQzPaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
          conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
          description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper_view")
  }

  /**
    * 解析试卷主题关联数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzCenterPaper(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCenterPaper.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val centerid = jsonObject.getIntValue("centerid")
        val openstatus = jsonObject.getString("openstatus")
        val sequence = jsonObject.getString("sequence")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center_paper")
  }

  /**
    * 解析做题试卷日志数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzPaper(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPaper.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val paperid = jsonObject.getIntValue("paperid")
        val papercatid = jsonObject.getIntValue("papercatid")
        val courseid = jsonObject.getIntValue("courseid")
        val paperyear = jsonObject.getString("paperyear")
        val chapter = jsonObject.getString("chapter")
        val suitnum = jsonObject.getString("suitnum")
        val papername = jsonObject.getString("papername")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val craetetime = jsonObject.getString("createtime")
        // 四舍五入保留一位小数
        val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val chapterid = jsonObject.getIntValue("chapterid")
        val chapterlistid = jsonObject.getIntValue("chapterlistid")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
          chapterlistid, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper")
  }

  /**
    * 解析主题数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzCenter(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCenter.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val centerid = jsonObject.getIntValue("centerid")
        val centername = jsonObject.getString("centername")
        val centeryear = jsonObject.getString("centeryear")
        val centertype = jsonObject.getString("centertype")
        val openstatus = jsonObject.getString("openstatus")
        val centerparam = jsonObject.getString("centerparam")
        val description = jsonObject.getString("description")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val sequence = jsonObject.getString("sequence")
        val provideuser = jsonObject.getString("provideuser")
        val centerviewtype = jsonObject.getString("centerviewtype")
        val stage = jsonObject.getString("stage")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
          sequence, provideuser, centerviewtype, stage, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center")
  }

  /**
    * 解析做题日志数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzQuestion(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzQuestion.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val questionid = jsonObject.getIntValue("questionid")
        val parentid = jsonObject.getIntValue("parentid")
        val questypeid = jsonObject.getIntValue("questypeid")
        val quesviewtype = jsonObject.getIntValue("quesviewtype")
        val content = jsonObject.getString("content")
        val answer = jsonObject.getString("answer")
        val analysis = jsonObject.getString("analysis")
        val limitminute = jsonObject.getString("limitminute")
        val status = jsonObject.getString("status")
        val optnum = jsonObject.getIntValue("optnum")
        val lecture = jsonObject.getString("lecture")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val modifystatus = jsonObject.getString("modifystatus")
        val attanswer = jsonObject.getString("attanswer")
        val questag = jsonObject.getString("questag")
        val vanalysisaddr = jsonObject.getString("vanalysisaddr")
        val difficulty = jsonObject.getString("difficulty")
        val quesskill = jsonObject.getString("quesskill")
        val vdeoaddr = jsonObject.getString("vdeoaddr")
        // 四舍五入保留一位小数
        val score = BigDecimal.apply(jsonObject.getDoubleValue("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val splitscore = BigDecimal.apply(jsonObject.getDoubleValue("splitscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为做题日志数据样例类
        DwdQzQuestion(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore,
          status, optnum, lecture, creator, createtime, modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill,
          vdeoaddr, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question")
  }

  /**
    * 解析题目类型数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzQuestionType(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzQuestionType.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val quesviewtype = jsonObject.getIntValue("quesviewtype")
        val viewtypename = jsonObject.getString("viewtypename")
        val questiontypeid = jsonObject.getIntValue("questypeid")
        val description = jsonObject.getString("description")
        val status = jsonObject.getString("status")
        val creator = jsonObject.getString("creator")
        val createtime = jsonObject.getString("createtime")
        val papertypename = jsonObject.getString("papertypename")
        val sequence = jsonObject.getString("sequence")
        val remark = jsonObject.getString("remark")
        val splitscoretype = jsonObject.getString("splitscoretype")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
          remark, splitscoretype, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question_type")
  }

  /**
    * 解析学员做题详情数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlQzMemberPaperQuestion(ssc: SparkContext, sparkSession: SparkSession): Unit = {
    // 隐式转换
    import sparkSession.implicits._
    // 数据操作
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzMemberPaperQuestion.log")
      // 过滤脏数据
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitions => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitions.map(item => {
        // 转换为JSONObject
        val jsonObject: JSONObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val userid = jsonObject.getIntValue("userid")
        val paperviewid = jsonObject.getIntValue("paperviewid")
        val chapterid = jsonObject.getIntValue("chapterid")
        val sitecourseid = jsonObject.getIntValue("sitecourseid")
        val questionid = jsonObject.getIntValue("questionid")
        val majorid = jsonObject.getIntValue("majorid")
        val useranswer = jsonObject.getString("useranswer")
        val istrue = jsonObject.getString("istrue")
        val lasttime = jsonObject.getString("lasttime")
        val opertype = jsonObject.getString("opertype")
        val paperid = jsonObject.getIntValue("paperid")
        val spendtime = jsonObject.getIntValue("spendtime")
        // 四舍五入小数点后保留一位小数
        val score = BigDecimal.apply(jsonObject.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
        val question_answer = jsonObject.getIntValue("question_answer")
        // 分区字段(按照建表声明的先后顺序获取)
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype,
          paperid, spendtime, score, question_answer, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_member_paper_question")
  }
}
