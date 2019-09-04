package com.atguigu.qzpoint.constant;

/**
 * @description:
 * @Author: my.yang
 * @Date: 2019/9/3 8:06 PM
 */
public class SqlConstantValue {

    /**
     * offset_manager Sql语句
     */
    public static final String SQL_PROXY_EXECTEQUERY_SELECT = "select * from `offset_manager` where groupid=?";
    public static final String SQL_PROXY_EXECTEQUERY_INTO = "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)";

    /**
     * qz_point_history Sql语句
     */
    public static final String SQL_QZ_POINT_HISTORY_SELECT = "select questionids from qz_point_history where userid=? and courseid=? and pointid=?";
    public static final String SQL_QZ_POINT_HISTORY_INSERT_AND_UPDATE = "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) on duplicate key update questionids=?,updatetime=?";
}
