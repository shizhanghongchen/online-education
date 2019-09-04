package com.atguigu.qzpoint.streaming

import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.atguigu.qzpoint.constant.SqlConstantValue
import com.atguigu.qzpoint.util.{ParseJsonData, _}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFiles}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @description: 页面转换率实时统计
  *               1. 需求1：计算首页总浏览数、订单页总浏览数、支付页面总浏览数;
  *               2. 需求2：计算商品课程页面到订单页的跳转转换率、订单页面到支付页面的跳转转换率;
  *               3. 需求3：根据ip得出相应省份,展示出top3省份的点击数,需要根据历史数据累加;
  * @Author: my.yang
  * @Date: 2019/9/4 7:07 PM
  */
object PageStreaming {
  def main(args: Array[String]): Unit = {

    /**
      * 获取配置文件工具类
      */
    val properties = PropertiesUtil.load("config.properties")

    /**
      * 设置Spark运行环境以及配置
      */
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)

      /**
        * 一次拉取最大的数据量(单个分区)
        */
      .set("spark.streaming.kafka.maxRatePerPartition", "30")

      /**
        * 背压 : 在有延时的情况下自动调整拉取的数据量
        */
      .set("spark.streaming.backpressure.enabled", "true")

      /**
        * 优雅的关闭
        */
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

      /**
        * 集群模式需要注释掉此代码
        */
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    /**
      * 设置主题
      */
    val topics = Array(properties.getProperty("page.topic"))

    // 获取KafkaMap
    val kafkaMap = MyKafkaUtil.getKafkaMap(properties.getProperty("page.group.id"))

    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client,
        SqlConstantValue.SQL_PROXY_EXECTEQUERY_SELECT,
        Array(properties.getProperty("page.group.id")), new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    // 设置kafka消费数据的参数,判断本地是否有偏移量(有则根据偏移量继续消费,无则重新消费)
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    // 解析json数据
    val dsStream = stream.map(item => item.value()).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        // 转换为元组
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    })
      // 过滤脏数据
      .filter(item => !item._5.equals("") && !item._6.equals("") && !item._7.equals(""))

    // 对数据做缓存
    dsStream.cache()
    // 改变结构
    dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
      // 根据key进行聚合
      .reduceByKey(_ + _)
      // 遍历每一个Rdd
      .foreachRDD(rdd => {
      // 遍历每一个分区
      rdd.foreachPartition(data => {
        // 在分区内获取jdbc连接
        val sqlProxy = new SqlProxy()
        // 获取连接
        val client = DataSourceUtil.getConnection
        try {
          // 计算页面跳转个数
          data.foreach(item => calcPageJumpCount(sqlProxy, item, client))
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    /**
      * 集群模式 : 需要将库文件上传到hdfs上,然后写入hdfs路径
      */
    // 广播文件
    ssc.sparkContext.addFile(this.getClass.getResource("/ip2region.db").getPath)
    // 对分区内数据进行操作
    val ipDStream = dsStream.mapPartitions(patitions => {
      val dbFile = SparkFiles.get("ip2region.db")
      val ipsearch = new DbSearcher(new DbConfig(), dbFile)
      // 结构转换
      patitions.map { item =>
        // 获取ip
        val ip = item._4
        // 获取ip详情并转换 : 中国|0|上海|上海市|有线通
        val province = ipsearch.memorySearch(ip).getRegion().split("\\|")(2)
        // 根据省份 统计点击个数
        (province, 1l)
      }
    })
      // 根据key进行聚合
      .reduceByKey(_ + _)

    // 对所有Rdd进行遍历
    ipDStream.foreachRDD(rdd => {
      // 查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlProxy()
      val ipClient = DataSourceUtil.getConnection
      try {
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple = (rs.getString(1), rs.getLong(2))
              // 汇总累加
              history_data += tuple
            }
          }
        })
        // 创建Rdd进行计算
        val history_rdd = ssc.sparkContext.makeRDD(history_data)
        // join
        val resultRdd = history_rdd.fullOuterJoin(rdd).map(item => {
          val province = item._1
          val nums = item._2._1.getOrElse(0l) + item._2._2.getOrElse(0l)
          // 转换为元组
          (province, nums)
        })
        // 遍历每一个分区
        resultRdd.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province = item._1
              val num = item._2
              // 修改mysql数据 并重组返回最新结果数据
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num)values(?,?) on duplicate key update num=?",
                Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        /**
          * 需求3 : 根据ip得出相应省份,展示出top3省份的点击数,需要根据历史数据累加;
          */
        val top3Rdd = resultRdd.sortBy[Long](_._2, false).take(3)
        // 写入数据库之前先清空数据
        sqlProxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        // 遍历写入
        top3Rdd.foreach(item => {
          sqlProxy.executeUpdate(ipClient, "insert into top_city_num (province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(ipClient)
      }
    })

    // 计算转换率
    // 处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        calcJumRate(sqlProxy, client) //计算转换率
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, SqlConstantValue.SQL_PROXY_EXECTEQUERY_INTO,
            Array(properties.getProperty("page.group.id"), or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 需求1 : 计算首页总浏览数、订单页总浏览数、支付页面总浏览数;
    *
    * @param sqlProxy
    * @param item
    * @param client
    */
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection): Unit = {
    // 切分字符串
    val keys = item._1.split("_")
    // 获取数量
    var num: Long = item._2
    // 获取当前page_id
    val page_id = keys(1).toInt
    // 获取上一page_id
    val last_page_id = keys(0).toInt
    // 获取下页面page_id
    val next_page_id = keys(2).toInt
    // 查询当前page_id的历史num个数
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          num += rs.getLong(1)
        }
        rs.close()
      }

      // 对num 进行修改 并且判断当前page_id是否为首页
      if (page_id == 1) {
        sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
          "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
      } else {
        sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
          "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, num))
      }
    })
  }

  /**
    * 需求2 : 计算商品课程页面到订单页的跳转转换率、订单页面到支付页面的跳转转换率;
    *
    * @param sqlProxy
    * @param client
    */
  def calcJumRate(sqlProxy: SqlProxy, client: Connection): Unit = {
    // 定义初始值
    var page1_num = 0l
    var page2_num = 0l
    var page3_num = 0l
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num = rs.getLong(1)
        }
      }
    })
    val nf = NumberFormat.getPercentInstance
    // 计算转换率 1 -> 2
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    // 计算转换率 2 -> 3
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
  }
}
