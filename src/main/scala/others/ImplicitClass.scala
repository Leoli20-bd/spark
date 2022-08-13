package others

import java.io.File
import scala.io.Source

/**
 * @ClassName: ImplicitMethod
 * @Author: haleli
 * @Date: 23:02
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object ImplicitClass {

  implicit class fileRead(file: File) {
    def read = Source.fromFile(file).mkString
  }

  def main(args: Array[String]): Unit = {
    val file = new File("/Users/haleli/myworld/project/java_pro/spark/src/main/Resources/application.conf")
    val read: String = file.read
    println(read)
  }

}
