package others

/**
 * @ClassName: ImplictDemo
 * @Author: haleli
 * @Date: 18:38
 * @ProjectName: spark
 * @Description: ${Desc}
 * @Version: ${Version}
 * */
object ImplicitPar {

  //隐式方法
  implicit def doubleToInt(i: Double): Int = {
    println("方法，double to int")
    i.toInt
  }

  //隐式变量
/*  implicit val v: Double => Int = {
    println("变量， double to int")
    _.toInt
  }*/

  val a: Int = 3.5



  //隐式变量
  implicit val b= 100

  def add(a:Int)(implicit b:Int,c:Int):Int = a + b

  def main(args: Array[String]): Unit = {

    println(a)
    println("========================分割线=========================")

    println(add(20))
  }


}

