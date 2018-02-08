package au.csiro.variantspark.work

import it.unimi.dsi.util.XorShift1024StarRandomGenerator

object StreamTest {
  
  def main(args:Array[String]) {
    Stream.iterate(0)( x =>  (x + 1) % 2).take(10).foreach(println)
    
    Range(0,23).toStream.sliding(10, 10).foreach(s => println(s.toList))

    val rng = new XorShift1024StarRandomGenerator(13L)
    
    
    val p = Array(1.2, 233.0, 1.0, 22)
    
    val s = p.toStream.zip(Stream.continually(rng.nextDouble()))
    
  }
}