package com.atguigu.qz.bean

/**
  * @description: dwd样例类
  * @Author: my.yang
  * @Date: 2019/8/31 10:52 AM
  */

/**
  * 知识点数据日志样例类
  *
  * @param pointid
  * @param courseid
  * @param pointname
  * @param pointyear
  * @param chapter
  * @param creator
  * @param createtime
  * @param status
  * @param modifystatus
  * @param excisenum
  * @param pointlistid
  * @param chapterid
  * @param sequence
  * @param pointdescribe
  * @param pointlevel
  * @param typelist
  * @param score
  * @param thought
  * @param remid
  * @param pointnamelist
  * @param typelistids
  * @param pointlist
  * @param dt
  * @param dn
  */
case class DwdQzPoint(pointid: Int,
                      courseid: Int,
                      pointname: String,
                      pointyear: String,
                      chapter: String,
                      creator: String,
                      createtime: String,
                      status: String,
                      modifystatus: String,
                      excisenum: Int,
                      pointlistid: Int,
                      chapterid: Int,
                      sequence: String,
                      pointdescribe: String,
                      pointlevel: String,
                      typelist: String,
                      score: BigDecimal,
                      thought: String,
                      remid: String,
                      pointnamelist: String,
                      typelistids: String,
                      pointlist: String,
                      dt: String,
                      dn: String)

/**
  * 试卷视图数据样例类
  *
  * @param paperviewid
  * @param paperid
  * @param paperviewname
  * @param paperparam
  * @param openstatus
  * @param explainurl
  * @param iscontest
  * @param contesttime
  * @param conteststarttime
  * @param contestendtime
  * @param contesttimelimit
  * @param dayiid
  * @param status
  * @param creator
  * @param createtime
  * @param paperviewcatid
  * @param modifystatus
  * @param description
  * @param papertype
  * @param downurl
  * @param paperuse
  * @param paperdifficult
  * @param testreport
  * @param paperuseshow
  * @param dt
  * @param dn
  */
case class DwdQzPaperView(paperviewid: Int,
                          paperid: Int,
                          paperviewname: String,
                          paperparam: String,
                          openstatus: String,
                          explainurl: String,
                          iscontest: String,
                          contesttime: String,
                          conteststarttime: String,
                          contestendtime: String,
                          contesttimelimit: String,
                          dayiid: Int,
                          status: String,
                          creator: String,
                          createtime: String,
                          paperviewcatid: Int,
                          modifystatus: String,
                          description: String,
                          papertype: String,
                          downurl: String,
                          paperuse: String,
                          paperdifficult: String,
                          testreport: String,
                          paperuseshow: String,
                          dt: String,
                          dn: String)

/**
  * 做题日志数据样例类
  *
  * @param questionid
  * @param parentid
  * @param questypeid
  * @param quesviewtype
  * @param content
  * @param answer
  * @param analysis
  * @param limitminute
  * @param scoe
  * @param splitcore
  * @param status
  * @param optnum
  * @param lecture
  * @param creator
  * @param createtime
  * @param modifystatus
  * @param attanswer
  * @param questag
  * @param vanalysisaddr
  * @param difficulty
  * @param quesskill
  * @param vdeoaddr
  * @param dt
  * @param dn
  */
case class DwdQzQuestion(questionid: Int,
                         parentid: Int,
                         questypeid: Int,
                         quesviewtype: Int,
                         content: String,
                         answer: String,
                         analysis: String,
                         limitminute: String,
                         scoe: BigDecimal,
                         splitcore: BigDecimal,
                         status: String,
                         optnum: Int,
                         lecture: String,
                         creator: String,
                         createtime: String,
                         modifystatus: String,
                         attanswer: String,
                         questag: String,
                         vanalysisaddr: String,
                         difficulty: String,
                         quesskill: String,
                         vdeoaddr: String,
                         dt: String,
                         dn: String)
