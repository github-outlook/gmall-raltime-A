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
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("canal_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL1122_C"
    val groupId = "gmall_base_db_canal_group"

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
        for (jsonObj <- jsonObjItr) {
          val dataArr: JSONArray = jsonObj.getJSONArray("data")
          for (i <- 0 to dataArr.size() - 1) {
            val dataJsonObj: JSONObject = dataArr.getJSONObject(i)
            val topic = "ODS_T_" + jsonObj.getString("table").toUpperCase()
            val id = dataJsonObj.getString("id")
            MyKafkaSink.send(topic, id, dataJsonObj.toString)

          }

        }
      }
      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
