package lishijia.spark.demo.sort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序，对文件中的多列排序
  */
object MoreSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MoreSort")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///home/lishijia/Documents/spark/sortdef/file", 1)
    val sortedResult = lines.map(line=>(new MoreSortKey(line.split(",")(1).toInt,
      line.split(",")(2).toInt),line))
      .sortByKey(false)
      .map(sortedLine =>sortedLine._2)
    sortedResult.collect().foreach (println)
  }

}
