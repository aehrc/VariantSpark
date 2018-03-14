package au.csiro.variantspark.work


class IntTest1(val l:Int) extends AnyVal {
  def value: Int = l
}


case class IntTest2(val k:Int) extends AnyVal {
  def data: Int = k
}


object AnyValTest {
 
  def main(argv:Array[String]) {
    println("Any val test")
 
    val k = Array(1,2,3)
    k(1)
    k(2) = 3
    
    
    val l: Int = 10
    val t1 = new IntTest1(l)
    t1.value
    
    val t2 = new IntTest2(3330)
    t2.data
    
    val p:Int = t2.k
  }
}