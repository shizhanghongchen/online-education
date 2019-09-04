package com.atguigu.qzpoint.streaming

import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.atguigu.qzpoint.constant.SqlConstantValue
import com.atguigu.qzpoint.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @description: 知识点掌握度实时统计 :
  *               需求1 : 要求Spark Streaming 保证数据不丢失,每秒100条处理速度,需要手动维护偏移量;
  *               需求2 : 同一个用户做在同一门课程同一知识点下做题需要去重,并且需要记录去重后的做题id与个数;
  *               需求3 : 计算知识点正确率,正确率计算公式 : 做题正确总个数 / 做题总数 (保留两位小数);
  *               需求4 : 计算知识点掌握度去重后的做题个数 / 当前知识点总题数（已知30题）* 当前知识点的正确率;
  * @Author: my.yang
  * @Date: 2019/9/4 8:47 AM
  */
object QzPointStreaming {

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
        * 模式切换 : 集群模式需注释此参数
        */
      .setMaster("local[*]")

      /**
        * 需求1 : 要求Spark Streaming 保证数据不丢失,每秒100条处理速度,需要手动维护偏移量;
        */
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置Topics
    val topics = Array(properties.getProperty("qz.log.topic"))
    // 获取KafkaMap
    val kafkaMap: Map[String, Object] = MyKafkaUtil.getKafkaMap(properties.getProperty("qzpoint.group.id"))

    // 查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    // 声明容器
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    // 获取连接
    val client = DataSourceUtil.getConnection
    try {
      // 代理执行
      sqlProxy.executeQuery(client,
        SqlConstantValue.SQL_PROXY_EXECTEQUERY_SELECT,
        Array(properties.getProperty("qzpoint.group.id")),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            // 关闭游标
            rs.close()
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 关闭
      sqlProxy.shutdown(client)
    }

    // 设置kafka消费数据的参数,判断本地是否有偏移量
    val stream: InputDStream[ConsumerRecord[String, String]] =
    // 没有偏移量重新消费
      if (offsetMap.isEmpty) KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
      // 有偏移量根据偏移量继续消费
      else KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))

    // 过滤脏数据
    val dsStream = stream.filter(item => item.value().split("\t").length == 6)
      // 在分区中操作 ---> mapPartitions
      .mapPartitions(data =>
      // 分区内数据进行map操作
      data.map(item => {
        // 分割数据
        val arr = item.value().split("\t")
        // 获取用户id
        val uid = arr(0)
        // 获取课程id
        val courseid = arr(1)
        // 获取知识点id
        val pointid = arr(2)
        // 获取题目id
        val questionid = arr(3)
        // 是否正确
        val istrue = arr(4)
        // 创建时间
        val createtime = arr(5)
        // 转换为元组
        (uid, courseid, pointid, questionid, istrue, createtime)
      }))

    // 遍历每一个时间间隔的RDD
    dsStream.foreachRDD(rdd => {
      // 获取相同用户 同一课程 同一知识点的数据
      val groupRdd = rdd.groupBy(item => item._1 + "-" + item._2 + "-" + item._3)
      // 遍历每一个时间间隔的RDD中的每一个partition
      groupRdd.foreachPartition(partition => {
        // 在分区下获取jdbc连接
        val sqlProxy = new SqlProxy()
        // 创建连接
        val client = DataSourceUtil.getConnection
        try {
          // 对题库进行更新操作
          partition.foreach { case (key, iters) => qzQuestionUpdate(key, iters, sqlProxy, client) }
        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client,
            SqlConstantValue.SQL_PROXY_EXECTEQUERY_INTO,
            Array(properties.getProperty("qzpoint.group.id"), or.topic, or.partition.toString, or.untilOffset))
        }
        //        for (i <- 0 until 100000) {
        //          val model = new LearnModel(1, 1, 1, 1, 1, 1, "", 2, 1l, 1l, 1, 1)
        //          map.put(UUID.randomUUID().toString, model)
        //        }
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
    * 对题目表进行更新操作
    *
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    * @return
    */
  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection) = {
    // 切分字段
    val keys = key.split("-")
    // 获取用户id
    val userid = keys(0).toInt
    // 获取课程id
    val courseid = keys(1).toInt
    // 获取知识点id
    val pointid = keys(2).toInt
    val array = iters.toArray
    // 对当前批次的数据下questionid 去重
    val questionids = array.map(_._4).distinct
    // 查询历史数据下的 questionid
    var questionids_history: Array[String] = Array()
    // 代理执行
    sqlProxy.executeQuery(client,
      SqlConstantValue.SQL_QZ_POINT_HISTORY_SELECT,
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
          }
          // 关闭游标
          rs.close()
        }
      })
    // 获取到历史数据后再与当前数据进行拼接 去重
    val resultQuestionid = questionids.union(questionids_history).distinct
    /**
      * 需求2 : 同一个用户做在同一门课程同一知识点下做题需要去重,并且需要记录去重后的做题id与个数;
      */
    //总数
    val countSize = resultQuestionid.length
    // ids
    val resultQuestionid_str = resultQuestionid.mkString(",")
    // 去重后的题个数
    val qz_count = questionids.length
    // 获取当前批次题总数
    var qz_sum = array.length
    // 获取当前批次做正确的题个数
    var qz_istrue = array.filter(_._5.equals("1")).size
    // 获取最早的创建时间 作为表中创建时间
    val createtime = array.map(_._6).min
    // 更新qz_point_set 记录表 此表用于存当前用户做过的questionid表
    val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
    sqlProxy.executeUpdate(client,
      SqlConstantValue.SQL_QZ_POINT_HISTORY_INSERT_AND_UPDATE,
      Array(userid, courseid, pointid, resultQuestionid_str, createtime, createtime, resultQuestionid_str, updatetime))

    // 初始化历史题总数
    var qzSum_history = 0
    // 初始化历史做正确的题个数
    var istrue_history = 0
    // 查询历史题总数及历史正确题个数
    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            qzSum_history += rs.getInt(1)
            istrue_history += rs.getInt(2)
          }
          rs.close()
        }
      })
    // 计算题总数 历史 + 当前
    qz_sum += qzSum_history
    // 计算正确题总数 历史 + 当前
    qz_istrue += istrue_history
    /**
      * 需求3 : 计算知识点正确率 正确率计算公式 : 做题正确总个数 / 做题总数 (保留两位小数);
      */
    val correct_rate = qz_istrue.toDouble / qz_sum.toDouble
    // 计算完成率
    // 假设每个知识点下一共有30道题  先计算题的做题情况 再计知识点掌握度
    // 算出做题情况乘以 正确率 得出完成率 假如30道题都做了那么正确率等于 知识点掌握度
    /**
      * 需求4 : 计算知识点掌握度 去重后的做题个数 / 当前知识点总题数（已知30题）* 当前知识点的正确率;
      */
    val qz_detail_rate = countSize.toDouble / 30
    val mastery_rate = qz_detail_rate * correct_rate
    // 执行更新
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))
  }
}