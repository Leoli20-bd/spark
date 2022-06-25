package sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

/**
  * @ClassName: DataSourceDemo
  * @Author: haleli
  * @Date: 18:38
  * @ProjectName: spark
  * @Description: ${Desc}
  * @Version: ${Version}
  **/
object DataSourceDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._


    val d1: DataFrame = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")

    d1.write.parquet("file:/Users/haleli/IdeaProjects/spark/data/test_table/key=1")

    val d2: DataFrame = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")

    d2.write.parquet("file:/Users/haleli/IdeaProjects/spark/data/test_table/key=2")

    val mergeTable: DataFrame = spark.read.option("mergeSchema", "true").parquet("file:/Users/haleli/IdeaProjects/spark/data/test_table")

    mergeTable.printSchema()

    mergeTable.createOrReplaceTempView("merge_table")

    spark.sql("select * from merge_table").show(50)

    //broadcastJoin
    //broadcast(spark.table("merge_table")).join(spark.table("merge_table"),"value").show()


    spark.close()


  }

}
