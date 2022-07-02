package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @ClassName: SparkWc
 * @Author: haleli
 * @Date: 14:47
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object SparkWc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    // 读取文件
    val rdd1: RDD[String] = spark.sparkContext.textFile("src/main/Resources/wc.text", 3)

    // 将段落转成单词
    val rdd2: RDD[String] = rdd1.flatMap(rf => rf.split(" "))

    // 将单个单词，转成key value格式
    val rdd3: RDD[(String, Int)] = rdd2.map(word => (word, 1))

    // 过滤掉不包含a,d的单词
    val rdd4: RDD[(String, Int)] = rdd3.filter(wm => wm._1.contains("a") || wm._1.contains("d"))

    // 将相同的key 结合计算总数
    val rdd5: RDD[(String, Int)] = rdd4.reduceByKey(_ + _)

    // 根据单词出现的次数排序
    val rdd6: RDD[(Int, String)] = rdd5.map(m => (m._2, m._1))
      .coalesce(1)
      .sortByKey(false)


    // 输出结果
    println("Count : " + rdd6.count())

    println("First :" + rdd6.first())

    println("Max :" + rdd6.max())

    println("Take : " + rdd6.take(3))

    println("collect : " + rdd6.collect())

    rdd6.foreach(r => println(r))

    rdd6.repartition(2)
      .map(x => {
        x._2 + " : "+x._1
      })
      .toDF("wc").as[String]
      .write
      .mode(SaveMode.Overwrite)
      .text("src/main/Resources/wc_rst")


    spark.close()
  }

}

