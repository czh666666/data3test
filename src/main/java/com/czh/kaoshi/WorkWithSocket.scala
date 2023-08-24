package com.czh.kaoshi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Author: fg
 * @Time: 2023-06-26 10:12
 * @function:
 *
 */
object WorkWithSocket {

  def main(args: Array[String]): Unit = {

    // 配置文件
    val sparkConf = new
        SparkConf().setMaster("local[*]").setAppName("Stream")

    // 创建streaming流，设置没每5 s 接收一次
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 创建kafka 消费者
    val ds = ssc.socketTextStream("hadoop102",9999)

    println("近10s 注册的用户数：")


    ds.map(_.split(" "))
      .filter(x => x(5) == "register")
      //  统计近10s
      .window(Seconds(10), Seconds(5))
      .count().print()


    // 启动 & 监听
    ssc.start()
    ssc.awaitTermination()
  }

}
