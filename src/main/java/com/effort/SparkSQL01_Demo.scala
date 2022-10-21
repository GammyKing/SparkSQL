package com.effort

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: xhz 
 * @Date: 2022/10/21 15:35 
 * @Project: IntelliJ IDEA
 * @Package: com.effort
 * @Version: 1.0.0
 * @Description: Create by xhz on 2022/10/21 15:35 
 * */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    // 创建上下文配置对象
    val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    // 创建SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._

    //读取json文件，创建DataFrame("username":"lisi","age":18)
    val df:DataFrame = spark.read.json("files/user.json")
    // SQL风格语法

    // DSL 风格语法

    //*****RDD=>DataFrame=>DataSet*****
    // RDD

    // DataFrame

    // DataSet

    //*****DataSet=>DataFrame=>RDD*****
    // DataFrame

    //RDD 返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集， 但是索引从 0 开始


  }

}
