package com.czh.kaoshi

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
/**
 *  从kafka接收数据，WordCount
 */
object Kafka_DataAnalize {
  def main(args: Array[String]): Unit = {

    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("MyTomcatLogCount")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      //若出现数据倾斜，调整以下这两个
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        List("topic_kaoshi"),
        Map(
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
          "group.id" -> "TomcatLog",
          //如果加入消费者组，从哪儿开始消费，  最早的数据
          "auto.offset.reset" -> "earliest",
          "enable.auto.commit" -> (false: lang.Boolean)
        ))
    )
    //这里面消费的是DStream
    //val ds: DStream[(String, Int)] =

//方法一：
      kafkaDStream
      .transform(
        rdd =>
          rdd.map(
            line => {
              val lines = line.value()
              //解析字符串，找到jsp的名字
              //1、得到两个双引号的位置
              val index1 = lines.indexOf("\"") //需要转义
              val index2 = lines.lastIndexOf("\"")
              val line1 = lines.substring(index1 + 1, index2) //得到两个空格的位置 GET /MyDemoWeb/oracle.jsp HTTP/1.1
              val index3 = line1.indexOf(" ")
              val index4 = line1.lastIndexOf(" ")
              val line2 = line1.substring(index3 + 1, index4) //  /MyDemoWeb/oracle.jsp

              //得到jsp的名字
              val jspName = line2.substring(line2.lastIndexOf("/") + 1) //得到xxx.jsp
              //返回
              (jspName, 1)
            }
          )
            //按照jsp的名字进行聚合操作
            .reduceByKey(_ + _)
            //排序，按照value进行排序
            .sortBy(_._2, false)
      ).print(2)




 /*       line => {
          //解析字符串，找到jsp的名字
          //1、得到两个双引号的位置
          val index1 = line.indexOf("\"") //需要转义
          val index2 = line.lastIndexOf("\"")
          val line1 = line.substring(index1 + 1, index2) //得到两个空格的位置 GET /MyDemoWeb/oracle.jsp HTTP/1.1
          val index3 = line1.indexOf(" ")
          val index4 = line1.lastIndexOf(" ")
          val line2 = line1.substring(index3 + 1, index4) //  /MyDemoWeb/oracle.jsp

          //得到jsp的名字
          val jspName = line2.substring(line2.lastIndexOf("/") + 1) //得到xxx.jsp
          //返回
          (jspName, 1)
        }
      )
      //按照jsp的名字进行聚合操作
      .reduceByKey(_ + _)
      //排序，按照value进行排序
      .sortBy(_._2, false)
*/
    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
