package spark

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, LongAccumulator}

/**
  * @ClassName: Agr
  * @Author: haleli
  * @Date: 01:17
  * @ProjectName: spark
  * @Description: ${Desc}
  * @Version: ${Version}
  **/
object Agr {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val myAccumulator = new MyAccumulator
    sc.register(myAccumulator, "myAccumulator")

    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("a", 6), ("c", 2), ("d", 1), ("b", 4)), 2)


    /*/*val agrrRdd: RDD[(String, Int)] = rdd.aggregateByK ``````ey(0)(Math.max(_, _), _ + _)

    agrrRdd.foreach(println)*/

    //第一个函数 进行分区内处理，第二个函数分区间相同数据处理
    //分区内部取相同k的最大值，分区间相同k进行相加
    rdd.aggregateByKey(0)((x, y) => {
      Math.max(x, y)
    }, (x, y) => x + y).foreach(println)*/



    val accRDD: RDD[(String, Int)] = rdd.map(x => {
      myAccumulator.add(x)
      x
    })
    accRDD.collect()
    println(myAccumulator.value)

  }

  class MyAccumulator extends AccumulatorV2[(String, Int), util.ArrayList[String]] {
    private var list: util.ArrayList[String] = new util.ArrayList[String]()

    override def isZero: Boolean = {
      list.isEmpty
    }

    override def copy(): AccumulatorV2[(String, Int), util.ArrayList[String]] = {
      val newAccumulator = new MyAccumulator
      newAccumulator.list = this.list
      newAccumulator
    }

    override def reset(): Unit = {
      list.clear()
    }

    override def add(v: (String, Int)): Unit = {
      list.add(v._1)
    }

    override def merge(other: AccumulatorV2[(String, Int), util.ArrayList[String]]): Unit = {
      list.addAll(other.value)
    }

    override def value: util.ArrayList[String] = {
      list
    }
  }

}
