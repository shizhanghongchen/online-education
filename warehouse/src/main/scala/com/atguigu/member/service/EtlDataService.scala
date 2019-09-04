package com.atguigu.member.service

import com.alibaba.fastjson.JSONObject
import com.atguigu.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * @description: dwd层逻辑
  * @Author: my.yang
  * @Date: 2019/8/30 7:36 PM
  */
object EtlDataService {

  /**
    * etl用户注册信息
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
    //隐式转换
    import sparkSession.implicits._
    val a = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/memberRegtype.log")
      // 过滤脏数据,转换为JSONObject判断是否为JSONObject
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partitoin => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partitoin.map(
        item => {
          // 转换为JSONObject
          val jsonObject = ParseJsonData.getJsonData(item)
          // 根据key获取不同字段
          val appkey = jsonObject.getString("appkey")
          val appregurl = jsonObject.getString("appregurl")
          val bdp_uuid = jsonObject.getString("bdp_uuid")
          val createtime = jsonObject.getString("createtime")
          val domain = jsonObject.getString("webA")
          val isranreg = jsonObject.getString("isranreg")
          val regsource = jsonObject.getString("regsource")
          // 模式匹配获取注册源
          val regsourceName = regsource match {
            case "1" => "PC"
            case "2" => "Mobile"
            case "3" => "App"
            case "4" => "WeChat"
            case _ => "other"
          }
          val uid = jsonObject.getIntValue("uid")
          val websiteid = jsonObject.getIntValue("websiteid")
          // 分区字段(按照建表声明的先后顺序获取)
          val dt = jsonObject.getString("dt")
          val dn = jsonObject.getString("dn")
          // 转换为元组
          (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
        })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
  }

  /**
    * etl用户表数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
    // 隐式转换
    import sparkSession.implicits._
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/member.log")
      // 过滤脏数据,转换为JSONObject判断是否为JSONObject
      .filter(item => ParseJsonData.getJsonData(item).isInstanceOf[JSONObject])
      // 对每一个分区的迭代器进行操作
      .mapPartitions(partition => {
      // 对分区内的每一条数据进行map(格式转换)操作
      partition.map(item => {
        // 转换为JSONObject
        val jsonObject = ParseJsonData.getJsonData(item)
        // 根据key获取不同字段
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val email = jsonObject.getString("email")
        // 脱敏字段 姓名格式 : 王xx
        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        // 脱敏字段 密码格式 : ******
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        // 脱敏字段 电话格式 : 137*****5451
        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getIntValue("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, dt, dn)
      })
    })
      // 转换为DataFrame手动减少DataFrame的partition数量且不触发shuffle,以追加的方式保存数据到hive表中
      .toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
  }

  /**
    * 导入广告表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
    //隐式转换
    import sparkSession.implicits._
    val result = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baseadlog.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val adid = jsonObject.getIntValue("adid")
        val adname = jsonObject.getString("adname")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (adid, adname, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  /**
    * 导入网站表基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baswewebsite.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        // 转换为元组
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }


  /**
    * 导入用户付款信息
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcentermempaymoney.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val paymoney = jSONObject.getString("paymoney")
        val uid = jSONObject.getIntValue("uid")
        val vip_id = jSONObject.getIntValue("vip_id")
        val site_id = jSONObject.getIntValue("siteid")
        val dt = jSONObject.getString("dt")
        val dn = jSONObject.getString("dn")
        // 转换为元组
        (uid, paymoney, site_id, vip_id, dt, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
  }

  /**
    * 导入用户vip基础数据
    *
    * @param ssc
    * @param sparkSession
    */
  def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
    // 隐式转换
    import sparkSession.implicits._
    ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcenterMemViplevel.log").filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jSONObject = ParseJsonData.getJsonData(item)
        val discountval = jSONObject.getString("discountval")
        val end_time = jSONObject.getString("end_time")
        val last_modify_time = jSONObject.getString("last_modify_time")
        val max_free = jSONObject.getString("max_free")
        val min_free = jSONObject.getString("min_free")
        val next_level = jSONObject.getString("next_level")
        val operator = jSONObject.getString("operator")
        val start_time = jSONObject.getString("start_time")
        val vip_id = jSONObject.getIntValue("vip_id")
        val vip_level = jSONObject.getString("vip_level")
        val dn = jSONObject.getString("dn")
        // 转换为元组
        (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }
}
