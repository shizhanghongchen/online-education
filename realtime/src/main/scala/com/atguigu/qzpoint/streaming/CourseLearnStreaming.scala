package com.atguigu.qzpoint.streaming

import java.sql.{Connection, ResultSet}

import com.atguigu.qzpoint.bean.LearnModel
import com.atguigu.qzpoint.constant.SqlConstantValue
import com.atguigu.qzpoint.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @description: 实时统计学员播放视频各时长
  *               1. 需求1 : 计算各章节下的播放总时长(按chapterid聚合统计播放总时长);
  *               2. 需求2 : 计算各课件下的播放总时长(按cwareid聚合统计播放总时长);
  *               3. 需求3 : 计算各辅导下的播放总时长(按edutypeid聚合统计播放总时长);
  *               4. 需求4 : 计算各播放平台下的播放总时长(按sourcetype聚合统计播放总时长);
  *               5. 需求5 : 计算各科目下的播放总时长(按subjectid聚合统计播放总时长);
  *               6. 需求6 : 计算用户学习视频的播放总时长、有效时长、完成时长,需求记录视频;
  *               播历史区间,对于用户多次学习的播放区间不累计有效时长和完成时长;
  *
  *               播放总时长计算 : (te-ts)/1000  向下取整  单位 : 秒;
  *               完成时长计算 : 根据pe-ps 计算 需要对历史数据进行去重处理;
  *               有效时长计算 : 根据te-ts 除以pe-ts 先计算出播放每一区间需要的实际时长 * 完成时长;
  * @Author: my.yang
  * @Date: 2019/9/4 8:14 PM
  */
object CourseLearnStreaming {
  def main(args: Array[String]): Unit = {
    /**
      * 获取配置文件工具类
      */
    val properties = PropertiesUtil.load("config.properties")

    /**
      * 设置Spark运行环境以及配置
      */
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "30")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    /**
      * 设置主题
      */
    val topics = Array(properties.getProperty("course.learn.topic"))

    /**
      * 获取KafkaMap
      */
    val kafkaMap = MyKafkaUtil.getKafkaMap(properties.getProperty("course.learn.group.id"))
    // 查询mysql是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client,
        SqlConstantValue.SQL_PROXY_EXECTEQUERY_SELECT,
        Array(properties.getProperty("course.learn.group.id")),
        new QueryCallback {
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
    // 设置kafka消费数据的参数 判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    // 解析json数据,操作分区内数据
    val dsStream = stream.mapPartitions(partitions => {
      // 对分区内数据进行结构转换
      partitions.map(item => {
        // 转换为JsonObject
        val jsonObject = ParseJsonData.getJsonData(item.value())
        // 获取各个字段
        val userId = jsonObject.getIntValue("uid")
        val cwareid = jsonObject.getIntValue("cwareid")
        val videoId = jsonObject.getIntValue("videoid")
        val chapterId = jsonObject.getIntValue("chapterid")
        val edutypeId = jsonObject.getIntValue("edutypeid")
        val subjectId = jsonObject.getIntValue("subjectid")
        // 播放设备来源
        val sourceType = jsonObject.getString("sourceType")
        // 速度
        val speed = jsonObject.getIntValue("speed")
        // 开始时间
        val ts = jsonObject.getLong("ts")
        // 结束时间
        val te = jsonObject.getLong("te")
        // 视频开始区间
        val ps = jsonObject.getIntValue("ps")
        // 视频结束区间
        val pe = jsonObject.getIntValue("pe")
        // 转换为样例类
        LearnModel(userId, cwareid, videoId, chapterId, edutypeId, subjectId, sourceType, speed, ts, te, ps, pe)
      })
    })

    // 遍历每一个Rdd
    dsStream.foreachRDD(rdd => {
      // 对rdd做缓存
      rdd.cache()
      // 统计播放视频 有效时长 完成时长 总时长
      // 根据key进行聚合
      rdd.groupBy(item => item.userId + "_" + item.cwareId + "_" + item.videoId)
        // 遍历每一个分区内的数据
        .foreachPartition(partitoins => {
        // 开启代理
        val sqlProxy = new SqlProxy()
        // 获取连接
        val client = DataSourceUtil.getConnection
        try {
          // 计算视频时长
          partitoins.foreach { case (key, iters) => calcVideoTime(key, iters, sqlProxy, client) }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      /**
        * 需求1 : 计算各章节下的播放总时长(按chapterid聚合统计播放总时长);
        */
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.chapterId
          (key, totaltime)
        })
      }).reduceByKey(_ + _)
        .foreachPartition(partitoins => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitoins.foreach(item => {
              sqlProxy.executeUpdate(client, "insert into chapter_learn_detail(chapterid,totaltime) values(?,?) on duplicate key" +
                " update totaltime=totaltime+?", Array(item._1, item._2, item._2))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })

      /**
        * 需求2 : 计算各课件下的播放总时长(按cwareid聚合统计播放总时长);
        */
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.cwareId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into cwareid_learn_detail(cwareid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      /**
        * 需求3 : 计算各辅导下的播放总时长(按edutypeid聚合统计播放总时长);
        */
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.edutypeId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into edutype_learn_detail(edutypeid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      /**
        * 需求4 : 计算各播放平台下的播放总时长(按sourcetype聚合统计播放总时长);
        */
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.sourceType
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into sourcetype_learn_detail (sourcetype_learn,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      /**
        * 需求5 : 计算各科目下的播放总时长(按subjectid聚合统计播放总时长);
        */
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.subjectId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitons => {
        val sqlProxy = new SqlProxy()
        val clinet = DataSourceUtil.getConnection
        try {
          partitons.foreach(item => {
            sqlProxy.executeUpdate(clinet, "insert into subject_learn_detail(subjectid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(clinet)
        }
      })

    })

    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, SqlConstantValue.SQL_PROXY_EXECTEQUERY_INTO,
            Array(properties.getProperty("course.learn.group.id"), or.topic, or.partition.toString, or.untilOffset))
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
    * 需求6 : 计算用户学习视频的播放总时长、有效时长、完成时长,需求记录视频;
    *
    * @param key
    * @param iters
    * @param sqlProxy
    * @param client
    */
  def calcVideoTime(key: String, iters: Iterable[LearnModel], sqlProxy: SqlProxy, client: Connection) = {
    // 分割数据
    val keys = key.split("_")
    val userId = keys(0).toInt
    val cwareId = keys(1).toInt
    val videoId = keys(2).toInt

    // 查询历史数据
    var interval_history = ""
    sqlProxy.executeQuery(client, "select play_interval from video_interval where userid=? and cwareid=? and videoid=?",
      Array(userId, cwareId, videoId), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            interval_history = rs.getString(1)
          }
          rs.close()
        }
      })
    // 初始化有效总时长
    var effective_duration_sum = 0l
    // 初始化完成总时长
    var complete_duration_sum = 0l
    // 初始化播放总时长
    var cumulative_duration_sum = 0l
    // 转成list 并根据开始区间升序排序
    val learnList = iters.toList.sortBy(item => item.ps)
    // 对排序后的数据遍历
    learnList.foreach(item => {
      // 没有历史区间数据
      if ("".equals(interval_history)) {
        // 有效区间
        val play_interval = item.ps + "-" + item.pe
        // 有效时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000)
        // 完成时长
        val complete_duration = item.pe - item.ps
        // 计算有效总时长
        effective_duration_sum += effective_duration.toLong
        // 计算播放总时长
        cumulative_duration_sum += effective_duration.toLong
        // 计算完成总时长
        complete_duration_sum += complete_duration
        // 将有效区间赋值给历史数据
        interval_history = play_interval
      }
      // 有历史区间数据进行对比
      else {
        // 对有效区间进行切分然后根据开始区间升序排序
        val interval_arry = interval_history.split(",").sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        // 计算有效区间
        val tuple = getEffectiveInterval(interval_arry, item.ps, item.pe)
        // 获取实际有效完成时长
        val complete_duration = tuple._1
        // 计算有效时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) / (item.pe - item.ps) * complete_duration
        // 累计时长
        val cumulative_duration = Math.ceil((item.te - item.ts) / 1000)
        // 将有效区间赋值给历史数据
        interval_history = tuple._2
        // 计算有效总时长
        effective_duration_sum += effective_duration.toLong
        // 计算完成总时长
        complete_duration_sum += complete_duration
        // 计算播放总时长
        cumulative_duration_sum += cumulative_duration.toLong
      }
      // 执行更新
      sqlProxy.executeUpdate(client, "insert into video_interval(userid,cwareid,videoid,play_interval) values(?,?,?,?) " +
        "on duplicate key update play_interval=?", Array(userId, cwareId, videoId, interval_history, interval_history))
      sqlProxy.executeUpdate(client, "insert into video_learn_detail(userid,cwareid,videoid,totaltime,effecttime,completetime) " +
        "values(?,?,?,?,?,?) on duplicate key update totaltime=totaltime+?,effecttime=effecttime+?,completetime=completetime+?",
        Array(userId, cwareId, videoId, cumulative_duration_sum, effective_duration_sum, complete_duration_sum, cumulative_duration_sum,
          effective_duration_sum, complete_duration_sum))
    })
  }

  /**
    * 计算有效区间
    *
    * @param array
    * @param start
    * @param end
    * @return
    */
  def getEffectiveInterval(array: Array[String], start: Int, end: Int) = {
    var effective_duration = end - start
    // 是否对有效时间进行修改
    var bl = false
    import scala.util.control.Breaks._
    breakable {
      for (i <- 0 until array.length) {
        // 循环各区间段
        var historyStart = 0 // 获取其中一段的开始播放区间
        var historyEnd = 0 // 获取其中一段结束播放区间
        val item = array(i)
        try {
          historyStart = item.split("-")(0).toInt
          historyEnd = item.split("-")(1).toInt
        } catch {
          case e: Exception => throw new Exception("error array:" + array.mkString(","))
        }
        if (start >= historyStart && historyEnd >= end) {
          // 已有数据占用全部播放时长 此次播放无效
          effective_duration = 0
          bl = true
          break()
        } else if (start <= historyStart && end > historyStart && end < historyEnd) {
          // 和已有数据左侧存在交集 扣除部分有效时间（以老数据为主进行对照）
          effective_duration -= end - historyStart
          array(i) = start + "-" + historyEnd
          bl = true
        } else if (start > historyStart && start < historyEnd && end >= historyEnd) {
          // 和已有数据右侧存在交集 扣除部分有效时间
          effective_duration -= historyEnd - start
          array(i) = historyStart + "-" + end
          bl = true
        } else if (start < historyStart && end > historyEnd) {
          // 现数据 大于旧数据 扣除旧数据所有有效时间
          effective_duration -= historyEnd - historyStart
          array(i) = start + "-" + end
          bl = true
        }
      }
    }
    val result = bl match {
      case false => {
        // 没有修改原array 没有交集 进行新增
        val distinctArray2 = ArrayBuffer[String]()
        distinctArray2.appendAll(array)
        distinctArray2.append(start + "-" + end)
        val distinctArray = distinctArray2.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            // 没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            // 有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
      case true => {
        // 修改了原array 进行区间重组
        val distinctArray = array.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            // 没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            // 有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
    }
    // 转换为元组
    (effective_duration, result)
  }
}
