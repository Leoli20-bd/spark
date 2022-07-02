package sparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

import java.util.Properties

/**
  * @ClassName: DataSourceDemo
  * @Author: haleli
  * @Date: 18:38
  * @ProjectName: spark
  * @Description: ${Desc}
  * @Version: ${Version}
  **/
object SparkRddTransform {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    //mysql properties
    val mysql_prop: Properties = new Properties()
    mysql_prop.setProperty("user","root")
    mysql_prop.setProperty("password","123456")

    //connect mysql
    val mysql: DataFrame = spark.read.jdbc("jdbc:localhost:3306", "default", mysql_prop)






    spark.close()


  }

}
