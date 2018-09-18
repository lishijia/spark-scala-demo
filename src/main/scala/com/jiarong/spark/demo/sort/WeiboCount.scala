package com.jiarong.spark.demo.sort
import org.apache.spark.{SparkConf, SparkContext}

object WeiboCount {
  //  data
  //  11111111    12743457
  //  11111111    16386587
  //  11111111    19764388
  //  11111111    12364375
  //  11111111    13426275
  //  11111111    12356363
  //  11111111    13256236
  //  11111111    10000032
  //  11111111    10000001
  //  11111112    10000001
  //  11111113    10000001
  //  11111114    12743457
//  （1）在spark-shell交互式环境中执行，请统计出一共有多少个不同的ID；
//  （2）在spark-shell交互式环境中执行，统计出一共有多少个不同的(ID,ID)对；
//  （3）在spark-shell交互式环境中执行，统计出每个用户的粉丝数量；
//  （4）使用Scala语言编写独立应用程序，统计出每个用户的粉丝数量，并且把统计结果写入到HDFS文件中。
//   其中，第1列和第2列都是表示用户ID，表中的数据是表示第1列的用户关注了第2列用户。

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WeiboCount"))
    //作业1
    //先过滤掉无用数据，然后再打平数据一个list，然后组装数据未key，value形式，然后统计
    //打平是因为需要把第一列也包含在统计数据里
    sc.textFile("file:///home/dev/spark/weiboCount/file").filter(x=>x.trim().length>0)
      .flatMap(x=>x.split('\t'))
      .map(userid=>(userid,1))
      .reduceByKey(_+_).count()

    //作业2
    //先过滤掉无用数据，然后组装数据未key，value形式，然后统计即可
    sc.textFile("file:///home/dev/spark/weiboCount/file").filter(x=>x.trim().length>0)
      .map(x=>(x,1))
      .reduceByKey(_+_).count

    //作业3
    sc.textFile("file:///home/dev/spark/weiboCount/file").filter(x=>x.trim().length>0)
      .map(x=>x.split('\t')(1))
      .map(x=>(x,1)).reduceByKey(_+_).collect

  }

}
