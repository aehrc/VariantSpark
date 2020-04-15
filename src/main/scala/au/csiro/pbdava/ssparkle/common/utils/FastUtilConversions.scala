package au.csiro.pbdava.ssparkle.common.utils
import scala.collection.JavaConverters._

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

class Long2DoubleOpenHashMap2Map(val ohm: Long2DoubleOpenHashMap) extends AnyVal {
  def asScala: Map[Long, Double] =
    ohm.entrySet().asScala.map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap

  def addAll(m2: Long2DoubleOpenHashMap): Long2DoubleOpenHashMap = {
    m2.asScala.foreach { case (i, v) => ohm.addTo(i, v) }
    ohm
  }

  def increment(i: Long, inc: Double): Long2DoubleOpenHashMap = {
    ohm.addTo(i, inc)
    ohm
  }
}

object FastUtilConversions {
  implicit def long2DoubleOpenHashMap2Map(
      ohm: Long2DoubleOpenHashMap): Long2DoubleOpenHashMap2Map =
    new Long2DoubleOpenHashMap2Map(ohm)

}
