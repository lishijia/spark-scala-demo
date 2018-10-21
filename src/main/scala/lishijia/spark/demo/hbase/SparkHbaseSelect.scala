package lishijia.spark.demo.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 查询hbase数据
  */
object SparkHbaseSelect {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkHbaseSelect"))

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum","hadoop100,hadoop101,hadoop102")
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    val stuRdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    val count = stuRdd.count()
    println("Student rdd count :" + count)
    stuRdd.cache()

    stuRdd.foreach({ case (_,result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
        val gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()))
        val age = Bytes.toString(result.getValue("info".getBytes(), "age".getBytes()))
        printf("row key:%s name:%s gender:%s age:%s", key, name, gender, age)
        println()
    })


  }

}
