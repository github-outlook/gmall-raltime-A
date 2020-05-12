package com.github_zhu.gmall.realtime.util.app

import java.lang
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.github_zhu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
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

    val startInputDstram: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    //    startInputDstram.map(_.value()).print(1000)

    //转换InputDStream[ConsumerRecord[String, String]] 结构
    val startJsonDstream: DStream[JSONObject] = startInputDstram.map { record =>
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

      val listJsonObj: List[JSONObject] = jsonObjItr.toList  //jsonObjItr只能迭代一次，
      println("过滤前"+listJsonObj.size)
      val jsonObjFilteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
      for (jsonObj <- listJsonObj) {
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(jsonObj.getLong("ts"))
        val dauKey = "dau" + ":" + dateStr
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val isFirstFlag: lang.Long = jedis.sadd(dauKey, mid)
        if(isFirstFlag ==1L){
          jsonObjFilteredList += jsonObj
        }
      }
      jedis.close()
      println("过滤后"+jsonObjFilteredList.size)
      //转换返回
      jsonObjFilteredList.toIterator
    }
     startJsonObjWithDauDstream.print(1000)

    //将数据插入 gmall_dau_info xxxx 中




    ssc.start()
    ssc.awaitTermination()
  }
}