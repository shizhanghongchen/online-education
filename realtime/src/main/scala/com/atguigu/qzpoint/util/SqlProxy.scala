package com.atguigu.qzpoint.util

import java.sql.{Connection, PreparedStatement, ResultSet}

/**
  * @description: Sql代理类
  * @Author: my.yang
  * @Date: 2019/9/3 4:29 PM
  */
class SqlProxy {

  /**
    * 声明空变量
    */
  private var rs: ResultSet = _
  private var psmt: PreparedStatement = _

  /**
    * 执行修改语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rtn = psmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
    * 执行查询语句
    *
    * @param conn
    * @param sql
    * @param params
    * @return
    */
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback) = {
    rs = null
    try {
      psmt = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          psmt.setObject(i + 1, params(i))
        }
      }
      rs = psmt.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 关闭
    *
    * @param conn
    */
  def shutdown(conn: Connection): Unit = DataSourceUtil.closeResource(rs, psmt, conn)
}

/**
  * 特质
  */
trait QueryCallback {
  def process(rs: ResultSet)
}
