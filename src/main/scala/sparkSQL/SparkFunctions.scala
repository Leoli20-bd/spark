package sparkSQL

import jline.internal.InputStreamReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.io.{BufferedInputStream, BufferedReader, File, FileInputStream, InputStream}
import java.sql.Date
import java.util.stream.Collectors

//link:sparkbyexamples.com

/**
 * @ClassName: SparkFunctions
 * @Author: haleli
 * @Date: 22:22
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object SparkFunctions {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()


    println(spark.version)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._
    import spark.sql

    //    // current_date()
    //    // 打印当前系统时间，返回类型date eg： 2022-06-22
    //    spark.sql("select current_date() as cnt_dt,current_timestamp() as cnt_ts").show(1)
    //
    //    // date_format(dateExpr:Column,format:String)
    //    // 将 date，timestamp 和date string 转换位其他格式时间，前提是date格式类型的
    //    spark.sql("select date_format(current_date(),'yyyyMMdd') as dt_fm,date_format(current_timestamp(),'yyyyMMdd HH:mm:ss.SSS') as ts_fm,date_format('2022-06-15','yyyyMMdd') as str_dt_fm,date_format('20220615','yyyy-MM-dd') str_not_fm").show(1)
    //
    //    // to_date(e:Column) , to_date(e:Column,fmt:String)
    //    // 将其他格式时间转换为，date格式
    //    spark.sql("select to_date('20120615','yyyyMMdd') as str_dt, to_date(current_timestamp()) as tm_dt").show(1)
    //
    //    // date_add(start:Column,days:Int) date_sub(start:Column,days:Int)
    //    // 数据格式为日期类型，并在当前日期的情况下，对时间加减
    //    spark.sql("select date_add(current_date(),1) dt_add, date_sub('2022-06-22',-1) str_dt_sub").show(1)
    //
    //    // from_unixtime(ut: Column, f: String)
    //    // 将bigint秒转换为指定格式字符串时间
    //    spark.sql("select from_unixtime(1655910258,'yyyy-MM-dd HH:mm:ss') as bt_fm").show(1)
    //
    //    // to_timestamp(s:Column) ,to_timestamp(s:Column,fmt:String)
    //    // 将时间转换为timestamp格式
    //    spark.sql("select to_timestamp('2022-06-23') as dt_tm,to_timestamp('20220623 12:00:12','yyyyMMdd HH:mm:ss') dt_fm_tm").show(1)


    val stream: BufferedInputStream = new BufferedInputStream(new FileInputStream(new File("config/sql/common.sql")))


    val Sqls: String = new BufferedReader(new InputStreamReader(stream)).lines().collect(Collectors.joining(System.lineSeparator()))

    val sql_arr: Array[String] = Sqls.split(System.lineSeparator())

    for (s <- sql_arr) {
      if (s != "") {
        println(s)
        spark.sql(s).show(1)
      }
    }


    val query_sql =
      """
        |select  10001 as id,
        |       'jack' as name,
        |       3000 as salary,
        |       current_date() as ct_dt
        |""".stripMargin

    val result: DataFrame = spark.sql(query_sql)
    //'simple', 'extended', 'codegen', 'cost', 'formatted'
    result.explain("cost")

    //print dataframe
    result.foreach(println(_))

    //print dataset
    val ds_row: Dataset[Row] = result.select("id", "name").where("name='jack'")
    ds_row.show(1)


    result.toDF("id", "name", "salary", "crt_dt").show(1)


    //rdd
    val rdd: RDD[Row] = result.rdd

    rdd.map(row => {
      employ(row.getAs[Int](0), row.getAs[String](1), row.getAs[Int](2), row.getAs[Date](3))
    }).take(1).foreach(println(_))


    //dataset
    val employ_ds: Dataset[employ] = result
      .toDF()
      .as[employ]

    employ_ds.select("id", "name", "salary").where("salary=3000")
      .show(1)

    // employ_ds.show(1)


    if (stream != null) {
      stream.close()
    }

    //从文件读取数据

    val schema =
      StructType(
        StructField("id", IntegerType, false) ::
          StructField("name", StringType, true) ::
          StructField("salary", IntegerType, true) :: Nil)

    val json_df: DataFrame = spark.read.schema(schema).json("config/data/employ.json").cache()

    json_df
      //.withColumn("salary",typedLit[Long](0))
      .groupBy("id", "name")
      .agg(collect_list("salary") as ("salary_list"))
      .show(10)

    spark.close()

  }

}

case class employ(id: Int, name: String, salary: Int, ct_dt: Date)