package au.csiro.variantspark.work

import au.csiro.pbdava.ssparkle.common.utils.Timed
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object BreezeTest {
  def main(argv:Array[String]) {
    
    val a = Array.fill(10)(1.0)
    
    val v = DenseVector(a)
    println(v)
    val k = v - DenseVector.fill(10)(0.5)
    k*=10.0
    println(k)
    println(a.toList)
    
  
  }
}