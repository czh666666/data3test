package com.czh.kaoshi

import java.lang


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *  从kafka接收数据，手动维护偏移量且追加历史数据
 */

object Kafka_DataAnalize2 {
  def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("MyTomcatLogCount")
          .master("local")
          .getOrCreate

        val sc: SparkContext = spark.sparkContext
        val ssc:StreamingContext = new StreamingContext(sc,Seconds(2))
        ssc.checkpoint("./ck")//设置检查点

        val streaming = KafkaUtils.createDirectStream[String, String](
          ssc,
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
            )
          )
        )
        streaming
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

          )
          //历史数据追加
          .updateStateByKey((nowBatch: Seq[Int], historyResult: Option[Int]) => Some(nowBatch.sum + historyResult.getOrElse(0)))
          .transform(
            line =>{
              line.sortBy(_._2,false)
            }
          )
          .print(2)

        //wordcount.updateStateByKey(Myfun)
        // .print(100)
        streaming.foreachRDD { rdd =>
          //获取该RDD对于的偏移量
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //更新偏移量
          // some time later, after outputs have completed(将偏移量更新【Kafka】)
          streaming.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }


        ssc.start
        ssc.awaitTermination



  }
}
