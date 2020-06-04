package com.github_zhu.gmall.realtime.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.github_zhu.gmall.realtime.bean.dim.BaseProvince
import com.github_zhu.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/18 8:17
 * @ModifiedBy:
 *
 */
/**
 * 保存kafka数据，维表到Hbase
 */
object BaseProvinceApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("base_province_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_T_BASE_PROVINCE"
    val groupId = "base_rovince_group"

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
    val baseProvinceDstream: DStream[BaseProvince] = inputGetOffsetsDstream.map { record =>
      val baseProvincJsonObjStr: String = record.value()
      val baseProvinc: BaseProvince = JSON.parseObject(baseProvincJsonObjStr, classOf[BaseProvince])
      baseProvinc
    }


    baseProvinceDstream.foreachRDD{rdd=>
      import org.apache.phoenix.spark._

      rdd.saveToPhoenix("GMALL1122_BASE_PROVINCE",Seq("ID", "NAME", "REGION_ID", "AREA_CODE")
        ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
