package au.csiro.variantspark.work


object StreamTest {
  
  def main(args:Array[String]) {
    Stream.iterate(0)( x =>  (x + 1) % 2).take(10).foreach(println)
    
    Range(0,23).toStream.sliding(10, 10).foreach(s => println(s.toList))
    
  }
}