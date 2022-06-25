package spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * @ClassName: TRDD
  * @Author: haleli
  * @Date: 13:01
  * @ProjectName: spark
  * @Description: ${Desc}
  * @Version: ${Version}
  **/
object TRDD {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("jack", 10000), ("rose", 20000), ("xiaoming", 12000), ("xiaobai", 40000), ("xiaohei", 16000)))

    val df: DataFrame = rdd.toDF("name", "salary")

    val ds1: Dataset[Empoly] = df.as[Empoly]

    val ds: Dataset[Empoly] = rdd.map(x => {
      Empoly(x._1, x._2)
    }).toDS()
    /*ds.createOrReplaceTempView("employ")

    spark.udf.register("MyAvg", new MyAvg)

    spark.sql("select MyAvg(salary) from employ").show()
*/
    val avgTwo = new MyAvgTwo
    val avgg: TypedColumn[Empoly, Double] = avgTwo.toColumn.name("avg")

    ds.select(avgg).show()


  }

  case class Empoly(name: String, salary: Long)

  case class Info(var sum: Long, var count: Long)

  class MyAvgTwo extends Aggregator[Empoly, Info, Double] {
    override def zero: Info = {
      new Info(0L, 0L)
    }

    override def reduce(b: Info, a: Empoly): Info = {
      b.count = b.count + 1
      b.sum = b.sum + a.salary
      b
    }

    override def merge(b1: Info, b2: Info): Info = {
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: Info): Double = {
      reduction.sum.toDouble / reduction.count.toDouble
    }

    override def bufferEncoder: Encoder[Info] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Double] = {
      Encoders.scalaDouble
    }
  }


  class MyAvg extends UserDefinedAggregateFunction {
    var sum = 0
    var count = 0

    override def inputSchema: StructType = {
      StructType(StructField("inputCloum", LongType) :: Nil)
    }

    override def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    override def dataType: DataType = {
      DoubleType
    }

    //Whether this function always returns the same output on the identical input
    override def deterministic: Boolean = {
      true
    }

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L;
      buffer(1) = 0L;
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
    }
  }


}
