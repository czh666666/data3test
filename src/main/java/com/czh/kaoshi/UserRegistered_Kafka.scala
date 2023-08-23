package com.czh.kaoshi

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserRegistered_Kafka {
  def main(args: Array[String]): Unit = {
    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("MyTomcatLogCount")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

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
    kafkaDStream.transform(
      rdd =>
        rdd.map(_.value().split(" "))
          .map(
            fields =>
              UserAction1(
                fields(0),
                fields(1).toLong,
                fields(2),
                fields(3),
                fields(4),
                fields(5)
              )
          )
          .filter(x => { x.behavior == "register"})
    )
      //  统计近10s
      .window(Seconds(10), Seconds(5))
      .count()
      .print()

    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
case class UserAction1(
                       date: String
                       , timestamp: Long
                       , userid: String
                       , pageid: String
                       , plate: String,
                       behavior: String
                     )
