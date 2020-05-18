package com.github_zhu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/13 16:34
 * @ModifiedBy:
 *
 */
object OffsetManager {
  // 把 redis中的偏移量读取出来，并转换成Kafka需要的偏移量格式 offsets:Map[TopicPartition,Long]
  def getOffsetMap(groupId: String, topic: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisCilent
    //redis tyoe hash  key? "offset:[groupid]:[topic]   fields? partition value? offset
    val offsetKey = "offset:" + groupId + ":" + topic
    //通过一个key查询hash所有值
    val redisOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    import scala.collection.JavaConversions._
    val kafkaOffsetMap: mutable.Map[TopicPartition, Long] = redisOffsetMap.map { case (partitionId, offsetStr) =>
      (new TopicPartition(topic, partitionId.toInt), offsetStr.toLong)
    }
    kafkaOffsetMap.toMap
  }

  def saveOffset(groupId: String, topic: String, offsetRanges: Array[OffsetRange]): Unit = {

    if (offsetRanges != null) {

      val jedis: Jedis = RedisUtil.getJedisCilent
      val offsetKey = "offset:" + groupId + ":" + topic
      val offsetMap = new util.HashMap[String, String]()

      //把每个分区的新的偏移量 提取组合
      var needSaveFlag = false

      for (offsetRang <- offsetRanges) {
        if (offsetRang.fromOffset < offsetRang.untilOffset) {
          needSaveFlag = true
        }
//        println("分区：" + offsetRang.partition + " from" + offsetRang.fromOffset + "->" + offsetRang.untilOffset)
        offsetMap.put(offsetRang.partition.toString, offsetRang.untilOffset.toString)
      }
      //把每个分区的新的的偏移量写入到redis
      if (needSaveFlag == true) {
        jedis.hmset(offsetKey, offsetMap)
      }
      jedis.close()
    }
  }
}
