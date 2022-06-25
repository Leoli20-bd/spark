package sparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    // current_date()
    // 打印当前系统时间，返回类型date eg： 2022-06-22
    spark.sql("select current_date() as cnt_dt,current_timestamp() as cnt_ts").show(1)

    // date_format(dateExpr:Column,format:String)
    // 将 date，timestamp 和date string 转换位其他格式时间，前提是date格式类型的
    spark.sql("select date_format(current_date(),'yyyyMMdd') as dt_fm,date_format(current_timestamp(),'yyyyMMdd HH:mm:ss.SSS') as ts_fm,date_format('2022-06-15','yyyyMMdd') as str_dt_fm,date_format('20220615','yyyy-MM-dd') str_not_fm").show(1)

    // to_date(e:Column) , to_date(e:Column,fmt:String)
    // 将其他格式时间转换为，date格式
    spark.sql("select to_date('20120615','yyyyMMdd') as str_dt, to_date(current_timestamp()) as tm_dt").show(1)

    // date_add(start:Column,days:Int) date_sub(start:Column,days:Int)
    // 数据格式为日期类型，并在当前日期的情况下，对时间加减
    spark.sql("select date_add(current_date(),1) dt_add, date_sub('2022-06-22',-1) str_dt_sub").show(1)

    // from_unixtime(ut: Column, f: String)
    // 将bigint秒转换为指定格式字符串时间
    spark.sql("select from_unixtime(1655910258,'yyyy-MM-dd HH:mm:ss') as bt_fm").show(1)




    spark.close()

  }

}
