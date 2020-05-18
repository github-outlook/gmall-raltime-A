package com.github_zhu.gmall.realtime.util.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.github_zhu.gmall.realtime.util.bean.DauInfo
import com.github_zhu.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext, TaskContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/12 19:20
 * @ModifiedBy:
 *
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "GMALL_START"
    val groupId = "GMALL_DAU_CONSUMER"
    //从redis 中读取偏移量
    val startOffset: Map[TopicPartition, Long] = OffsetManager.getOffsetMap(groupId, topic)
    var startInputDstram: InputDStream[ConsumerRecord[String, String]] = null
    //判断如果从redis中读取的当前最新偏移量则用偏移量加载kafka中数据，否则直接从kafka中读取默认最新的数据
    if (startOffset != null && startOffset.size > 0) {
      startInputDstram = MyKafkaUtil.getKafkaStream(topic, ssc, startOffset, groupId)
    } else {
      startInputDstram = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //    startInputDstram.cache()
    //    startInputDstram.map(_.value()).print(100)
    //获取本批次偏移量的移动后的新位置
    var startOffsetRanges: Array[OffsetRange] = null
    val startupInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstram.transform { rdd =>
      startOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //    startInputDstram.map(_.value()).print(1000)

    //转换InputDStream[ConsumerRecord[String, String]] 结构
    val startJsonDstream: DStream[JSONObject] = startupInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObjecct: JSONObject = JSON.parseObject(jsonString)
      jSONObjecct
    }

    //写入去重清单 日活 每天一个清单 key: 每天一个key
    //
    //      startJsonDstream.map { jsonObj => //方式一 链接性能消耗大
    //      //redis 写入  type set key dau:2020-05-12  value mid
    //      val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
    //
    //      val dauKey = "dau" + dateStr
    //      val jedis: Jedis = new Jedis("hadoop102", 6379)
    //      val mid: String = jsonObj.getJSONObject("common").getString("mid")
    //      jedis.sadd(dauKey, mid)
    //      jedis.close()
    //    }
    //
    //    startJsonDstream.mapPartitions { jsonObjItr =>
    //      val jedis: Jedis = new Jedis("hadoop102", 6379) //方式二  每个分区数据创建一个链接
    //      for (jsonObj <- jsonObjItr) {
    //        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
    //        val dauKey = "dau" + dateStr
    //        val mid: String = jsonObj.getJSONObject("common").getString("mid")
    //        jedis.sadd(dauKey, mid)
    //      }
    //      jedis.close()
    //
    //    }

    val startJsonObjWithDauDstream: DStream[JSONObject] = startJsonDstream.mapPartitions { jsonObjItr =>

      val jedis: Jedis = RedisUtil.getJedisCilent

      val listJsonObj: List[JSONObject] = jsonObjItr.toList //jsonObjItr只能迭代一次，

      println("过滤前" + listJsonObj.size)

      val jsonObjFilteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()

      for (jsonObj <- listJsonObj) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
        val dauKey = "dau" + ":" + dateStr
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey,3600*24*7)
        if (isFirstFlag == 1L) {
          jsonObjFilteredList += jsonObj
        }
      }
      jedis.close()
      println("过滤后" + jsonObjFilteredList.size)
      //转换返回
      jsonObjFilteredList.toIterator
    }
    //    startJsonObjWithDauDstream.print(1000)

    //将数据插入 gmall_dau_info xxxx 中
    //1.转换结构
    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDstream.map { JSONObj =>
      val commonJsonObj: JSONObject = JSONObj.getJSONObject("common")
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(JSONObj.getLong("ts")))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)

      DauInfo(commonJsonObj.getString("mid"),
        commonJsonObj.getString("uid"),
        commonJsonObj.getString("ar"),
        commonJsonObj.getString("ch"),
        commonJsonObj.getString("vc"),
        dt, hr, mi, JSONObj.getLong("ts")
      )
    }
    //2.将数据插入  gmall_dau_info xxxx 中
    dauInfoDstream.foreachRDD { rdd =>
      rdd.foreachPartition { rddItr =>

        //观察偏移量变化
        val offsetRange: OffsetRange = startOffsetRanges(TaskContext.getPartitionId())
        println("偏移量：" + offsetRange.fromOffset + "->" + offsetRange.untilOffset)

        //写入ES//存放（key：mid , value:dauInfo）
        val dataList: List[(String, DauInfo)] = rddItr.toList.map { dauInfo => (dauInfo.mid, dauInfo) }
        val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
        val indexName = "gmall_dau_info_" + dt
        MyEsUtil.saveBulk(dataList, indexName)

      }
      //保存偏移量
      OffsetManager.saveOffset(groupId, topic, startOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}