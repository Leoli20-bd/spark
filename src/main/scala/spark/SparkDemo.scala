package spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark")
      .master("local[2]")
      .getOrCreate()


    val sc = spark.sparkContext

    val conf = new SparkConf()
    val file = sc.textFile("/Users/haleli/newWorld/fileInfo/pandas/ex1.csv")
    file.foreach(println(_))
    val infoRdd = file.map(x => {
      val sp = x.split("\\,")
      Info(sp(0), sp(1), sp(2), sp(3), sp(4))
    }).repartition(2)

    /*file.flatMap(x=>{
      val list = new ListBuffer[(String,Int)]z
      val sp: mutable.ArrayOps[String] = x.split(" ")
      for(s<-sp){
        list.append((s,1))
      }
      list
    }).reduceByKey(_+_)*/


    file.map((_,1))
      .reduceByKey((x1,x2)=>{(x1+x2)})


    import spark.implicits._
    val infoDF = infoRdd.toDF()
    //infoDF.write.format("parquet").mode("overwrite").save("/Users/haleli/newWorld/fileInfo/pandas/test")
    //infoDF.write.parquet("/Users/haleli/newWorld/fileInfo/pandas/test2")
    //infoDF.write.orc("/Users/haleli/newWorld/fileInfo/pandas/orc")
    //val df = spark.read.orc("/Users/haleli/newWorld/fileInfo/pandas/orc")
    //infoDF.write.csv("/User/haleli/newWorld/fileInfo/pandas/csv")
    //spark.read.csv("/User/haleli/newWorld/fileInfo/pandas/csv")
    val coll = infoDF.rdd.collect()
    println(coll)
    var i = 0
    for(s<-coll){
      println(s)
      i = i +1
    }
    println(i)

    infoDF.show(3)

    infoDF.write.mode(SaveMode.Overwrite)

    spark.close()
  }
}

case class Info(a: String, b: String, c: String, d: String, message: String)