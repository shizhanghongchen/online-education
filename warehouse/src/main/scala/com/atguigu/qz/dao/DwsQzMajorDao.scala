package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @description: dws主修维度宽表数据类
  * @Author: my.yang
  * @Date: 2019/8/31 3:34 PM
  */
object DwsQzMajorDao {

  /**
    * 查询dwd.dwd_qz_major基础数据 ---> 主修数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getQzMajor(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator," +
      s"createtime as major_createtime,dt,dn from dwd.dwd_qz_major where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_website基础数据 ---> 做题网站日志数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getQzWebsite(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select siteid,sitename,domain,multicastserver,templateserver,creator," +
      s"createtime,multicastgateway,multicastport,dn from dwd.dwd_qz_website where dt='$dt'")
  }

  /**
    * 查询dwd.dwd_qz_business基础数据 ---> 所属行业数据表
    *
    * @param sparkSession
    * @param dt
    * @return
    */
  def getQzBusiness(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='$dt'")
  }
}
