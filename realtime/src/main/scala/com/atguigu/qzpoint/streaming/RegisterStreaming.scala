package com.atguigu.qzpoint.streaming

import java.sql.ResultSet

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
  * @description: 实时统计注册人员信息
  *               需求1：实时统计注册人数批次为3秒一批,使用updateStateBykey算子计算历史数据和当前批次的数据总数(仅此需求使用
  *               updateStateBykey后续需求不使用updateStateBykey);
  *               需求2：每6秒统计一次1分钟内的注册数据,不需要历史数据(reduceByKeyAndWindow);
  * @Author: my.yang
  * @Date: 2019/9/3 3:44 PM
  */
object RegisterStreaming {

  def main(args: Array[String]): Unit = {

    /**
      * 获取配置文件工具类
      */
    val properties = PropertiesUtil.load("config.properties")

    // 设置用户解决权限问题
    System.setProperty("HADOOP_USER_NAME", properties.getProperty("hadoop.user.name"))

    // 设置Spark运行环境以及配置
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置Topics
    val topics = Array(properties.getProperty("register.topic"))
    // 获取KafkaMap
    val kafkaMap: Map[String, Object] = MyKafkaUtil.getKafkaMap(properties.getProperty("register.group.id"))
    // 设置checkpoint支持updateStateByKey算子
    ssc.checkpoint(properties.getProperty("update.start.string"))

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
        Array(properties.getProperty("register.group.id")),
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

    // 过滤数据 : 舍弃分割后不符合长度的数据
    val resultDstream = stream.filter(item => item.value().split("\t").length == 3)
      // 在分区中操作 ---> mapPartitions
      .mapPartitions(datas => {
      // 分区内数据进行map操作
      datas.map(item => {
        // 获取当前行并通过"\t"切割获取数组,模式匹配获取注册渠道
        val app_name = item.value().split("\t")(1) match {
          case "1" => "PC"
          case "2" => "APP"
          case _ => "Other"
        }
        // 封装元组返回
        (app_name, 1)
      })
    })
    // 使用缓存避免重复计算
    resultDstream.cache()

    /**
      * 需求2：每6秒统计一次1分钟内的注册数据,不需要历史数据(reduceByKeyAndWindow);
      */
    resultDstream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()

    /**
      * 需求1：实时统计注册人数批次为3秒一批,使用updateStateBykey算子计算历史数据和当前批次的数据总数
      */
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      // 本批次求和
      val currentCount = values.sum
      // 历史数据
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    resultDstream.updateStateByKey(updateFunc).print()

    /**
      * 需求2：每6秒统计一次1分钟内的注册数据,不需要历史数据(reduceByKey);
      */
    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          (rand.nextInt(3) + "_" + app_id, 1)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    /**
      * 处理完业务逻辑后,手动提交offset维护到本地mysql中
      */
    stream.foreachRDD(rdd => {
      // 代理执行
      val sqlProxy = new SqlProxy()
      // 创建连接
      val client = DataSourceUtil.getConnection
      try {
        // 获取偏移量offsets
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 循环提交offset
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client,
            SqlConstantValue.SQL_PROXY_EXECTEQUERY_INTO,
            Array(properties.getProperty("register.group.id"), or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        // 关闭连接
        sqlProxy.shutdown(client)
      }
    })

    // 执行
    ssc.start()
    ssc.awaitTermination()
  }
}
