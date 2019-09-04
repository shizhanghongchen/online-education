package com.atguigu.member.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dwd dao
  * @Author: my.yang
  * @Date: 2019/8/30 8:19 PM
  */
object DwdMemberDao {

  /**
    * 查询主表用户表
    *
    * @param sparkSession
    * @return
    */
  def getDwdMember(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id,email,fullname,iconurl,lastlogin,mailaddr,memberlevel," +
      "password,phone,qq,register,regupdatetime,unitname,userip,zipcode,dt,dn from dwd.dwd_member")
  }

  /**
    * 查询用户注册数据
    *
    * @param sparkSession
    * @return
    */
  def getDwdMemberRegType(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,appkey,appregurl,bdp_uuid,createtime as reg_createtime,domain,isranreg," +
      "regsource,regsourcename,websiteid as siteid,dn from dwd.dwd_member_regtype ")
  }

  /**
    * 查询基础广告表数据
    *
    * @param sparkSession
    * @return
    */
  def getDwdBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql("select adid as ad_id,adname,dn from dwd.dwd_base_ad")
  }

  /**
    * 查询基础网站表数据
    *
    * @param sparkSession
    * @return
    */
  def getDwdBaseWebSite(sparkSession: SparkSession) = {
    sparkSession.sql("select siteid,sitename,siteurl,delete as site_delete," +
      "createtime as site_createtime,creator as site_creator,dn from dwd.dwd_base_website")
  }

  /**
    * 查询vip基础数据
    *
    * @param sparkSession
    * @return
    */
  def getDwdVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql("select vip_id,vip_level,start_time as vip_start_time,end_time as vip_end_time," +
      "last_modify_time as vip_last_modify_time,max_free as vip_max_free,min_free as vip_min_free," +
      "next_level as vip_next_level,operator as vip_operator,dn from dwd.dwd_vip_level")
  }

  /**
    * 查询用户支付情况记录
    *
    * @param sparkSession
    * @return
    */
  def getDwdPcentermemPayMoney(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,cast(paymoney as decimal(10,4)) as paymoney,vip_id,dn from dwd.dwd_pcentermempaymoney")
  }
}
