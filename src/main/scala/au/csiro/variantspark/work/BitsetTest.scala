package au.csiro.variantspark.work

import scala.collection.immutable.BitSet
import scala.collection.mutable.{BitSet => MBitSet}

object BitsetTest {
  
  def main(args:Array[String]) {
    
    val ordinalValues = Array(1,2,3,4,1,0,10)
    val builder = BitSet.newBuilder
    builder.sizeHint(ordinalValues.length)
    ordinalValues.foreach(builder.+=)
    val set = builder.result()
    set.foreach(println)
   
    
    val mset = MBitSet()
    mset++=ordinalValues
    mset.foreach(println)
    
  }
}