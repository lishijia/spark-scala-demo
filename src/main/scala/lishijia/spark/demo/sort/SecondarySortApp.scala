package lishijia.spark.demo.sort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 指定一个文件当中的某列进行排序
  */
object SecondarySortApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SecondarySortApp")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:///usr/local/spark/mycode/rdd/file1.txt", 1)
    val pairWithSortKey = lines.map(line=>(new SecondarySortKey(line.split(" ")(0).toInt,
      line.split(" ")(1).toInt),line))
    val sorted = pairWithSortKey.sortByKey(false)
    val sortedResult = sorted.map(sortedLine =>sortedLine._2)
    sortedResult.collect().foreach (println)


  }

}
