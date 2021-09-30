package au.csiro.pbdava.ssparkle.common.utils
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}

import scala.collection.JavaConverters._

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

class Long2LongOpenHashMap2Map(val ohm: Long2LongOpenHashMap) extends AnyVal {
  def asScala: Map[Long, Long] =
    ohm.entrySet().asScala.map(e => (e.getKey.toLong, e.getValue.toLong)).toMap

  def addAll(m2: Long2LongOpenHashMap): Long2LongOpenHashMap = {
    m2.asScala.foreach { case (i, v) => ohm.addTo(i, v) }
    ohm
  }

  def increment(i: Long, inc: Long): Long2LongOpenHashMap = {
    ohm.addTo(i, inc)
    ohm
  }
}

object FastUtilConversions {
  implicit def long2DoubleOpenHashMap2Map(
      ohm: Long2DoubleOpenHashMap): Long2DoubleOpenHashMap2Map =
    new Long2DoubleOpenHashMap2Map(ohm)

  implicit def long2LongOpenHashMap2Map(ohm: Long2LongOpenHashMap): Long2LongOpenHashMap2Map =
    new Long2LongOpenHashMap2Map(ohm)
}
