package com.jiarong.spark.demo.sort

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 读取hdfs目录下的多个文件的内容排序
  */
object FileSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FileSort")
    val sc = new SparkContext(conf)


    /**
      * 文件1 33 37 12 40
      * 文件2 4 16 39 5
      * 文件3 1 45 25
      */

    val lines = sc.textFile("hdfs://hadoop100:9000/jiarong/input/spark/filesort",3)

    var index = 0;
    val result = lines.filter(x=>x.trim.length>0).map(x=>(x.toInt,""))
      .partitionBy(new HashPartitioner(1)).sortByKey().map(x=>{
      index = index + 1
      (index, x._1)
    })
    result.saveAsTextFile("hdfs://hadoop100:9000/jiarong/output/spark/filesort")

  }

}
