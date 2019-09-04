package com.atguigu.member.service

import com.atguigu.member.bean.QueryResult
import com.atguigu.member.dao.DwsMemberDao
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

/**
  * @description: ads service
  * @Author: my.yang
  * @Date: 2019/8/30 8:38 PM
  */
object AdsMemberService {

  /**
    * 统计各项指标 : api方式实现
    *
    * @param sparkSession
    */
  def queryDetailApi(sparkSession: SparkSession, dt: String) = {
    // 隐式转换
    import sparkSession.implicits._
    val result: Dataset[QueryResult] = DwsMemberDao.queryIdlMemberData(sparkSession).as[QueryResult].where(s"dt='${dt}'")
    // 缓存优化,避免重复查询
    result.cache()
    // 统计注册来源url人数
    val a = result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (appregurl, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")
    // 统计所属网站人数
    result.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues((item => item._2)).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")
    // 统计所属来源人数 pc mobile wechat app
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")
    // 统计通过各广告进来的人数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")
    // 统计各用户等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")
    // 统计各用户vip等级人数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")
    // 统计各memberlevel等级 支付金额前三的用户
    import org.apache.spark.sql.functions._
    result.withColumn("rownum", row_number().over(Window.partitionBy("dn", "memberlevel").orderBy(desc("paymoney"))))
      .where("rownum<4").orderBy("memberlevel", "rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")
  }


  /**
    * 统计各项指标 使用sql
    *
    * @param sparkSession
    */
  def queryDetailSql(sparkSession: SparkSession, dt: String) = {

    /**
      * 统计注册来源url人数
      */
    val appregurlCount = DwsMemberDao.queryAppregurlCount(sparkSession, dt)

    /**
      * 统计所属网站人数
      */
    val siteNameCount = DwsMemberDao.querySiteNameCount(sparkSession, dt)

    /**
      * 统计所属来源人数
      */
    val regsourceNameCount = DwsMemberDao.queryRegsourceNameCount(sparkSession, dt)

    /**
      * 统计通过各广告注册的人数
      */
    val adNameCount = DwsMemberDao.queryAdNameCount(sparkSession, dt)

    /**
      * 统计各用户等级人数
      */
    val memberLevelCount = DwsMemberDao.queryMemberLevelCount(sparkSession, dt)

    /**
      * 统计用户各个等级人数
      */
    val vipLevelCount = DwsMemberDao.queryMemberLevelCount(sparkSession, dt)

    /**
      * 统计各memberlevel等级,支付金额前三的用户
      */
    val top3MemberLevelPayMoneyUser = DwsMemberDao.getTop3MemberLevelPayMoneyUser(sparkSession, dt).show()
  }
}
