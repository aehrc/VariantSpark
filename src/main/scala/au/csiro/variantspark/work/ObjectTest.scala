package au.csiro.variantspark.work


object ObjectTest {
  
  def main(argv:Array[String]) {
    println("Hello")
    
    val x = List("cc", "yy", "xx")
    
    x match {
      case a :: b :: _ => println("xxx")
    }
  
  }
}