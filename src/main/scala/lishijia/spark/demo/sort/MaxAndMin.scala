package lishijia.spark.demo.sort

import org.apache.spark.{SparkConf, SparkContext}
object MaxAndMin {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MaxAndMin")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://hadoop100:9000/jiarong/input/spark/maxandmin/*")
    lines.filter(x=>x.trim().length>0).map(x=>("", x.trim.toInt)).groupByKey().map(x=>{
      var index =0
      var min = 0;
      var max =0
      for(num <- x._2){
        if(index==0){
          min=num
          max=num
          index = index + 1
        }

        if(num<min){
          min=num;
        }

        if(max<num){
          max = num
        }
      }
      (min,max)
    }).collect().foreach(x=>{
      printf("the min is := %d, the max is := %d", x._1, x._2)
      println()
    })

  }

}
