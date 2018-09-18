package com.jiarong.spark.demo.sort
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计hdfs当中某个目录下所有文件的top值
  */
object TopValue {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TopValue")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://hadoop100:9000/jiarong/input/spark/*",2)
    lines.filter(x=>x.trim.length>0 && x.split(",").length==4)
      .map(_.split(","))
      .map(x=>(x(2).toInt,"")).sortByKey(false).take(5).foreach(x=>{
      printf("the payment is %d", x._1)
      println()
    })

  }

}
