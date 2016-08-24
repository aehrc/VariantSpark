package au.csiro.pbdava.ssparkle.common.utils
import scala.collection.JavaConversions._

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap


class Long2DoubleOpenHashMap2Map(val ohm:Long2DoubleOpenHashMap) extends AnyVal {
  def asScala:Map[Long,Double] = 
    ohm.entrySet().map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap

  def addAll(m2:Long2DoubleOpenHashMap):Long2DoubleOpenHashMap =  {
      m2.foreach { case (i, v) => ohm.addTo(i,v) }
      ohm
  }
  
  def increment(i:Long, inc:Double) = {
    ohm.addTo(i, inc)
    ohm
  }
}

object FastUtilConversions {
  implicit def long2DoubleOpenHashMap2Map(ohm:Long2DoubleOpenHashMap):Long2DoubleOpenHashMap2Map = 
      new Long2DoubleOpenHashMap2Map(ohm)
  
  
}