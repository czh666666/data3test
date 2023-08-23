package com.czh.kaoshi
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
object DataAnalize {
  def main(args: Array[String]): Unit = {

    //创建一个Context对象//创建一个Context对象

    val conf = new SparkConf().setAppName("MyTomcatLogCount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.textFile("data/kaoshi.txt")
      .map(
        line => {
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
    println("访问量最大的两个网页是:\n" + rdd.take(2).toBuffer)

    sc.stop()
  }
}
