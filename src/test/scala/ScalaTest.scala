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
    var s : Option[String] = Some(null)

   // s = Some("1")

    println(s)
    println(s.get)
    println(s.getOrElse("c"))

  }


}
