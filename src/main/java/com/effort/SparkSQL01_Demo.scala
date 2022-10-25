package com.effort

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sparkproject.jetty.server.Authentication.User

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
    // 创建SparkSession对象 使用sparksql时需要引入session对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._

    //读取json文件，创建DataFrame("username":"lisi","age":18)
    val df:DataFrame = spark.read.json("files/user.json")
    df.show()
    // SQL风格语法
    df.createOrReplaceTempView("user")
    // DSL 风格语法
    df.select("username","age").show()



    //*****RDD=>DataFrame=>DataSet*****
    // RDD
    val rdd1:RDD[(Int,String,Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",28),(3,"wangwu",20)))
    // DataFrame
    val df1:DataFrame = rdd1.toDF("id","name","age")
    //df1.show()

    // DataSet
    val ds1:Dataset[User] = df1.as[User]
    //ds1.show()


    //*****DataSet=>DataFrame=>RDD*****
    // DataFrame
    val df2:DataFrame = ds1.toDF()
    //RDD 返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集， 但是索引从 0 开始
    val rdd2:RDD[Row] = df2.rdd
    rdd2.foreach(a=>println(a.getString(1)))

    //***********RDD=>DataSet************
    rdd1.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

  spark.stop()

  }

}

case class User(id: Int, name: String, age: Int)
