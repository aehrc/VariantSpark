package au.csiro.variantspark.input

import scala.reflect.ClassTag

class HashingLabelSource(val maxLabel: Int) extends ResponseSource {
  def getResponses(sampleIds: Seq[String]): Array[Int] =
    sampleIds.map { id =>
      val normalized = (id.hashCode().toDouble / Int.MaxValue.toDouble + 1.0) / 2.0
      (normalized * maxLabel).toInt.min(maxLabel - 1)
    }.toArray
  def getResponses[T: ClassTag](sampleIds: Seq[String], convert: String => T): Array[T] =
    getResponses(sampleIds).map(i => convert(i.toString))
}

class HashingValueSource(val minValue: Double = 0.0, val maxValue: Double = 1.0)
    extends ResponseSource {
  def getResponses(sampleIds: Seq[String]): Array[Double] =
    sampleIds.map { id =>
      val normalized = (id.hashCode().toDouble / Int.MaxValue.toDouble + 1.0) / 2.0
      minValue + normalized * (maxValue - minValue)
    }.toArray
  def getResponses[T: ClassTag](sampleIds: Seq[String], convert: String => T): Array[T] =
    getResponses(sampleIds).map(d => convert(d.toString))
}
