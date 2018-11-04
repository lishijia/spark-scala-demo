package lishijia.spark.demo.sort

class MoreSortKey(val first:Int, val second:Int)
  extends Ordered [MoreSortKey] with Serializable{

  override def compare(that: MoreSortKey): Int ={
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }

}
