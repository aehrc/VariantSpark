package au.csiro.variantspark.utils.tests

import au.csiro.pbdava.ssparkle.common.utils.Timed
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object BreezeTest {
  def main(argv:Array[String]) {
    
    
    val v = DenseVector.zeros[Double](1000000)
    Timed.time {
      Range(0, 1000).foreach  { _ =>
        //val a = v.toArray
        val d = v.data
      }
    }.report("Breeze: toArray")
  
    
   val sv = Vectors.dense(v.toArray)
    Timed.time {
      Range(0, 1000).foreach  { _ =>
        val a = sv.toArray
      }
    }.report("Spark: toArray")    

    
  
  }
}