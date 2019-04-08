package au.csiro.variantspark.work

object RegexTest {
  def main(args:Array[String]) {
    
    val xx = s"$${cc}"
    println(xx)
    
    println("XXXX")
     val date = """(([^_]+)_([^_]+))_([^_]+)""".r
     "xx_xxxxyy_zz" match {
      case date(a,b,c,_*) => println(a, b, c)
    }
    
    
    
  }
}