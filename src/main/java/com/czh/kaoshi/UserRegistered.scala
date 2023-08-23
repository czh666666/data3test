package com.czh.kaoshi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserRegistered {
  def main(args: Array[String]): Unit = {
    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTest")
    //5秒一个批次
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.checkpoint("./ck")

    val dStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 8888)
    //包装成样例类
    dStream
      .map(_.split(" "))
      .map(
        fields =>
          UserAction(
            fields(0),
            fields(1).toLong,
            fields(2),
            fields(3),
            fields(4),
            fields(5)
          )
      )
      .filter(x => { x.behavior == "register"})
      //  统计近10s
      .window(Seconds(10), Seconds(5))
      .count()
      .print()

    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
case class UserAction(
                       date: String
                       , timestamp: Long
                       , userid: String
                       , pageid: String
                       , plate: String,
                       behavior: String
                     )
