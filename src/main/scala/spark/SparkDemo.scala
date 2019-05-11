package spark

import org.apache.spark.sql.SparkSession


object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("spark")
      .master("local[1]")
      .getOrCreate()

    val sc = spark.sparkContext
    val file = sc.textFile("/Users/haleli/newWorld/fileInfo/pandas/ex1.csv")
    file.foreach(println(_))
    val infoRdd = file.map(x => {
      val sp = x.split("\\,")
      Info(sp(0), sp(1), sp(2), sp(3), sp(4))
    })


    import spark.implicits._
    val infoDF = infoRdd.toDF()
    //infoDF.write.format("parquet").mode("overwrite").save("/Users/haleli/newWorld/fileInfo/pandas/test")
    //infoDF.write.parquet("/Users/haleli/newWorld/fileInfo/pandas/test2")
    //infoDF.write.orc("/Users/haleli/newWorld/fileInfo/pandas/orc")
    //val df = spark.read.orc("/Users/haleli/newWorld/fileInfo/pandas/orc")
    infoDF.write.csv("/User/haleli/newWorld/fileInfo/pandas/csv")

    spark.close()
  }
}

case class Info(a: String, b: String, c: String, d: String, message: String)