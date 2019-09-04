package com.atguigu.qzpoint.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @description: 德鲁伊连接池
 * @Author: my.yang
 * @Date: 2019/9/3 4:30 PM
 */
public class DataSourceUtil implements Serializable {

    /**
     * 声明静态变量
     */
    public static DataSource dataSource = null;

    /**
     * 使用静态块进行初始化
     */
    static {
        try {
            Properties props = new Properties();
            // 配置连接池
            props.setProperty("url", ConfigurationManager.getProperty("jdbc.url"));
            props.setProperty("username", ConfigurationManager.getProperty("jdbc.user"));
            props.setProperty("password", ConfigurationManager.getProperty("jdbc.password"));
            props.setProperty("initialSize", "5"); //初始化大小
            props.setProperty("maxActive", "10"); //最大连接
            props.setProperty("minIdle", "5");  //最小连接
            props.setProperty("maxWait", "60000"); //等待时长
            props.setProperty("timeBetweenEvictionRunsMillis", "2000");//配置多久进行一次检测,检测需要关闭的连接 单位毫秒
            props.setProperty("minEvictableIdleTimeMillis", "600000");//配置连接在连接池中最小生存时间 单位毫秒
            props.setProperty("maxEvictableIdleTimeMillis", "900000"); //配置连接在连接池中最大生存时间 单位毫秒
            props.setProperty("validationQuery", "select 1");
            props.setProperty("testWhileIdle", "true");
            props.setProperty("testOnBorrow", "false");
            props.setProperty("testOnReturn", "false");
            props.setProperty("keepAlive", "true");
            props.setProperty("phyMaxUseCount", "100000");
            // 连接池不需要指定驱动类
//            props.setProperty("driverClassName", "com.mysql.jdbc.Driver");
            // 创建连接池
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 提供获取连接的方法
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 1. 提供关闭资源的方法【connection是归还到连接池】
     * 2. 提供关闭资源的方法 【方法重载】3 dql
     *
     * @param resultSet
     * @param preparedStatement
     * @param connection
     */
    public static void closeResource(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        // 关闭结果集
        closeResultSet(resultSet);
        // 关闭语句执行者
        closePrepareStatement(preparedStatement);
        // 关闭连接
        closeConnection(connection);
    }

    /**
     * 关闭连接
     *
     * @param connection
     */
    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭语句执行者
     *
     * @param preparedStatement
     */
    private static void closePrepareStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭结果集
     *
     * @param resultSet
     */
    private static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
