package com.atguigu.qzpoint.bean

/**
  * @description:
  * @Author: my.yang
  * @Date: 2019/9/4 9:07 PM
  */

/**
  * 学习样例类
  *
  * @param userId
  * @param cwareId
  * @param videoId
  * @param chapterId
  * @param edutypeId
  * @param subjectId
  * @param sourceType
  * @param speed
  * @param ts
  * @param te
  * @param ps
  * @param pe
  */
case class LearnModel(
                       userId: Int,
                       cwareId: Int,
                       videoId: Int,
                       chapterId: Int,
                       edutypeId: Int,
                       subjectId: Int,
                       sourceType: String,
                       speed: Int,
                       ts: Long,
                       te: Long,
                       ps: Int,
                       pe: Int
                     )
