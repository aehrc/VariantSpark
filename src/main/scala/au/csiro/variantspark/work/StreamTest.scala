package au.csiro.variantspark.work


object StreamTest {
  
  def main(args:Array[String]) {
    Stream.iterate(0)( x =>  (x + 1) % 2).take(10).foreach(println)
  }
}