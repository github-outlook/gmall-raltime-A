package com.github_zhu.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/5/12 19:31
 * @ModifiedBy:
 *
 */
object PropertiesUtil {

  def main(args: Array[String]): Unit = {

    val properties: Properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load (propertiesName:String):Properties={
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertiesName),"UTF-8"))
    prop
  }
}
