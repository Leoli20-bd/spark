import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/**
 * @ClassName: Test
 * @Author: haleli
 * @Date: 10:53
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object ScalaTest {
  def main(args: Array[String]): Unit = {
    var s: Option[String] = Some(null)

    // s = Some("1")

    println(s)
    println(s.get)
    println(s.getOrElse("c"))

    val str = "Spark SQL"
    val array: Array[Char] = str.toCharArray
    var result = ""
    val len: Int = array.length-1
    for (i <- 0 to (len)) {
      result += Integer.toBinaryString(array(i)) + ""
    }

    println(result)
    println(result.length)


    println("-----------")
    val ss = "spark-sql"
    val str1: String = "".padTo(ss.length, ' ')
    println(str1)

    println("-----------")

    /*val file = new File("")
    require(file.exists())*/

    val config: Config = ConfigFactory.load()

    println(config)

    new NullPointerException

  }


}
