package au.csiro.variantspark.work

import breeze.linalg.DenseVector

object BreezeTest {
  def main(argv:Array[String]) {
    
    val a = Array.fill(10)(1.0)
    
    val v = DenseVector(a)
    println(v)
    v*=2.0
    val k = v - DenseVector.fill(10)(0.5)
    k*=10.0
    println(k)
    println(a.toList)
    
  
  }
}