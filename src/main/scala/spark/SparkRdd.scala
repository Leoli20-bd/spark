package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: SparkRdd
 * @Author: haleli
 * @Date: 12:50
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object SparkRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark")
      .master("local[4]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val oneRDD: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d","a","b"))
      .map(_ * 2)
      .repartition(1)


    val result: (String, Int) = oneRDD.map((_, 1))
      .fold("aa",5)((x,y)=>{
        (x._1,x._2+y._2)
      })

    println(result)

    println(oneRDD.countByValue())

    val otherRDD: RDD[String] = sc.makeRDD(Array("b", "c", "d", "e", "f", "g", "h"))
      .map(_ * 2)
      .repartition(3)

    val unionRdd: RDD[String] = oneRDD.union(otherRDD)

    unionRdd.partitions.map(x=>{
      val i: Int = x.index
      println(s"i : $i")
    })
    unionRdd.dependencies.foreach(println(_))

    unionRdd.mapPartitions(x=>{

      x.map(y=>{

      })
    })

    unionRdd.collect()

    sc.stop()

  }

}
