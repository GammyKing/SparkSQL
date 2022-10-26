package com.effort

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable




/**
 * @Author: xhz 
 * @Date: 2022/10/26 15:34 
 * @Project: IntelliJ IDEA
 * @Package: com.effort
 * @Version: 1.0.0
 * @Description: Create by xhz on 2022/10/26 15:34 
 * */
object SparkStreaming_demo {
  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming_demo")

    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    // 3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // 4。创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

    // 5.处理队列中的RDD数据
    val mapperDStream = inputStream.map((_, 1))
    val reducerDStream = mapperDStream.reduceByKey(_ + _)

    // 6.打印结果
    reducerDStream.print()

    // 7.启动任务
    ssc.start()

    // 8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
