package au.csiro.variantspark.work


object ObjectTest {
  
  def main(argv:Array[String]) {
    println("Hello")
    
    println(Array().indexOf(1))
    println(Array(2).zipWithIndex.find(_._1 > 1 ).map(_._2).getOrElse(1))
  }
}