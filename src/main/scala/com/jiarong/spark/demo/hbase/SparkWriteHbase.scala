package com.jiarong.spark.demo.hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

/**
  * spark 写入数据至hbase数据
  */
object SparkWriteHbase {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("SparkWriteHbase")
    val sc = new SparkContext(sconf)

    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "student")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","hadoop100,hadoop101,hadoop102")

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val inDataRdd = sc.makeRDD(Array("3,Rongcheng,M,26","4,Guanhua,M,27"))
    val rdd = inDataRdd.map(_.split(",")).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt))
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
    print("save to hbase ok")


  }

}
