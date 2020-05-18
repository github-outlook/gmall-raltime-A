package com.github_zhu.gmall.realtime.util.app.dw

import com.alibaba.fastjson.{JSON, JSONObject}
import com.github_zhu.gmall.realtime.util.bean.{OrderInfo, UserState}
import com.github_zhu.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/16 21:02
 * @ModifiedBy:
 *
 */
object OrderInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_order_info_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_T_ORDER_INFO"
    val groupId = "base_order_info_group"

    //读取redis中的偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManager.getOffsetMap(groupId, topic)

    //加载数据
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsets, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetsDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    /*^^^^^^^^^^^^^^^^业务处理^^^^^^^^^^^^^^^^^^^^^^^^*/
    //基本转换  补充日期字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetsDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      val timeArr = dateTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      orderInfo
    }
    val orderInfoWithFirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        val userList: List[String] = orderInfoList.map(_.user_id.toString)
        val sql = "select user_id,if_consumed from user_state1122 where user_id in ('" + userList.mkString("','") + "')"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

        //避免2重循环  把一个list转成map
        val userStateMap: Map[String, String] = userStateList.map(userStateJsonObj =>
          //注意返回字段大小写
          (userStateJsonObj.getString("USER_ID"), userStateJsonObj.getString("IF_CONSUMED"))
        ).toMap
        for (orderInfo <- orderInfoList) {

          val userIfConsumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
          if (userIfConsumed != null && userIfConsumed == "1") {
            orderInfo.if_first_order = "0"

          } else {
            orderInfo.if_first_order = "1"
          }
        }
      }
      orderInfoList.toIterator
    }
    //解决同一批次多次下单问题  如果首次消费，躲避订单会被认为是首单
    val orderInfoWithUidDstream: DStream[(Long, OrderInfo)] = orderInfoWithFirstDstream.map { orderInfo => (orderInfo.user_id, orderInfo) }
    val orderInfoGroupByUidDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithUidDstream.groupByKey()
    val orderInfoFinalFirstDstream: DStream[OrderInfo] = orderInfoGroupByUidDstream.flatMap { case (userId, orderInfoItr) =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList(0).if_first_order == "1" && orderInfoList.size > 1) { //有首单标志的用户订单集合进行处理
        //订单按照时间进行排序
        val orderInfoSortedList: List[OrderInfo] = orderInfoList.sortWith { (order1, order2) => order1.create_time < order2.create_time }
        for (i <- 1 to orderInfoSortedList.size - 1) {
          orderInfoSortedList(i).if_first_order = "0"
        }
        orderInfoSortedList.toIterator
      } else {
        orderInfoList.toIterator
      }
    }

    //    查询次数过多
    //    orderInfoFinalFirstDstream.map { orderInfo =>
    //      val sql = "select id,name,region_id,area_code from gmall1122_base_province where id=' " + orderInfo.user_id + "'"
    //      val provinceJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
    //      if (provinceJsonList != null && provinceJsonList.size > 0) {
    //        orderInfo.province_name = provinceJsonList(0).getString("NAME")
    //        orderInfo.province_area_code = provinceJsonList(0).getString("AREA_CODE")
    //      }
    //      orderInfo
    //    }
    val sql = "select id,name,region_id,area_code from gmall1122_base_province"
    val provinceJsonList: List[JSONObject] = PhoenixUtil.queryList(sql)
    orderInfoFinalFirstDstream.map { orderInfo =>

      if (provinceJsonList != null && provinceJsonList.size > 0) {
        orderInfo.province_name = provinceJsonList(0).getString("NAME")
        orderInfo.province_area_code = provinceJsonList(0).getString("AREA_CODE")
      }
      orderInfo
    }


    orderInfoFinalFirstDstream.foreachRDD { rdd =>
      import org.apache.phoenix.spark._

      val userStateRDD: RDD[UserState] = rdd.filter(_.if_first_order == "1").map { orderInfo => UserState(orderInfo.user_id.toString, orderInfo.if_first_order) }


      userStateRDD.saveToPhoenix("user_state1122",
        Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )

      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }

    /* orderInfoDstream.foreachRDD { rdd =>
   rdd.foreachPartition { jsonObjItr =>
     for (jsonObj <- jsonObjItr) {
       val dataObj = jsonObj.getJSONObject("data")
       val tableName: String = jsonObj.getString("table")
       val id: String = dataObj.getString("id")
       val topic = "ODS_T_" + tableName.toUpperCase()
       MyKafkaSink.send(topic, id, dataObj.toString)
     }
   }
   OffsetManager.saveOffset(groupId, topic, offsetRanges)
 }*/
    ssc.start()
    ssc.awaitTermination()
  }
}


