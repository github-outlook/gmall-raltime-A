package com.github_zhu.gmall.realtime.app.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.github_zhu.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/15 20:32
 * @ModifiedBy:
 *
 */
object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_maxwell_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL1122_M"
    val groupId = "gmall_base_db_maxwell_group"

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
    val dbJsonObjDstream: DStream[JSONObject] = inputGetOffsetsDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }


    dbJsonObjDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonObjItr =>
       /* for (jsonObj <- jsonObjItr) {
          val dataObj = jsonObj.getJSONObject("data")
          val tableName: String = jsonObj.getString("table")
          val id: String = dataObj.getString("id")
          val topic = "ODS_T_" + tableName.toUpperCase()

          //          if (!jsonObj.getString("type").equals("bootstrap-complete") && !jsonObj.getString("type").equals("bootstrap-start")) {
          if (dataObj != null && dataObj.size() > 0) {
            if  (dataObj != null && !dataObj.isEmpty && !jsonObj.getString("type").equals("delete"))
              if ((tableName == "order_info" && jsonObj.getString("type").equals("insert"))
                || (tableName == "base_province")
                ||(tableName == "user_info")
                ||(tableName == "spu_info")
                ||(tableName == "sku_info")
                ||(tableName == "base_category3")
                ||(tableName == "base_trademark")
                ||((tableName == "order_detail") )
              ) {
                if(tableName == "user_info"||tableName == "user_detail"){
                  Thread.sleep(100)
                }
              MyKafkaSink.send(topic, id, dataObj.toString)
            }
          }
        }*/

        for (jsonObj <- jsonObjItr) {
          val dataObj: JSONObject = jsonObj.getJSONObject("data")
          val tableName = jsonObj.getString("table")
          val id = dataObj.getString("id")
          val topic = "ODS_T_" + tableName.toUpperCase
          if (dataObj != null && !dataObj.isEmpty && !jsonObj.getString("type").equals("delete"))
            if ((tableName == "order_info" && jsonObj.getString("type").equals("insert"))
              || (tableName == "base_province")
              ||(tableName == "user_info")
              ||(tableName == "spu_info")
              ||(tableName == "sku_info")
              ||(tableName == "base_category3")
              ||(tableName == "base_trademark")
              ||((tableName == "order_detail") )
            ) {
              //              if(tableName == "order_info"|| tableName == "order_detail" ){
              //                  Thread.sleep(300)
              //              }
              MyKafkaSink.send(topic, id, dataObj.toJSONString)
            }


        }

      }
    }
    OffsetManager.saveOffset(groupId, topic, offsetRanges)
    ssc.start()
    ssc.awaitTermination()

  }
}
