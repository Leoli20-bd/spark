package sparkSQL

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * @ClassName: MyUDF
  * @Author: haleli
  * @Date: 22:46
  * @ProjectName: spark
  * @Description: ${Desc}
  * @Version: ${Version}
  **/
object MyUDF {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.sparkContext.makeRDD(Seq((11,"a"),(22,"a"),(3,""),(4,"b"))).toDF("id","word")

    df.createOrReplaceTempView("df_table")

    spark.udf.register("replaceNull",replaceNull _)

    spark.sql("select id, replaceNull(word) from df_table").show()

    df.write.mode(SaveMode.Overwrite).csv("/Users/haleli/IdeaProjects/spark/data/t")

    spark.close()

  }


  def replaceNull(x:String):String={
    var tmp:String = ""
    if(x!=null){
      tmp = x
    }
    tmp
  }

}
