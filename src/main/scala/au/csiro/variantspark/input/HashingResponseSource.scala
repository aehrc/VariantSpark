package au.csiro.variantspark.input

class HashingLabelSource(val maxLabel: Int) extends LabelSource {
  def getResponses(sampleIds: Seq[String]): Array[Int] =
    sampleIds.map { id =>
      val normalized = (id.hashCode().toDouble / Int.MaxValue.toDouble + 1.0) / 2.0
      (normalized * maxLabel).toInt.min(maxLabel - 1)
    }.toArray
}

class HashingValueSource(val minValue: Double = 0.0, val maxValue: Double = 1.0)
    extends ValueSource {
  def getResponses(sampleIds: Seq[String]): Array[Double] =
    sampleIds.map { id =>
      val normalized = (id.hashCode().toDouble / Int.MaxValue.toDouble + 1.0) / 2.0
      minValue + normalized * (maxValue - minValue)
    }.toArray
}
