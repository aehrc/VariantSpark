package au.csiro.variantspark.input

object Missing {
  val BYTE_NA_VALUE: Byte = (-1).toByte
  def isNA(v: Byte): Boolean = (BYTE_NA_VALUE == v)
  def isNotNA(v: Byte): Boolean = (BYTE_NA_VALUE != v)
  def replaceNA(v: Byte, inputedValue: Byte): Byte = {
    if (isNA(v)) inputedValue else v
  }
}

trait ImputationStrategy {
  def impute(data: Array[Byte]): Array[Byte]
}

case object DisabledImputationStrategy extends ImputationStrategy {
  override def impute(data: Array[Byte]): Array[Byte] = {
    if (data.exists(Missing.isNA)) {
      throw new IllegalArgumentException(
          "Missing values present in data but imputation is not enabled.")
    }
    data
  }
}

case object ZeroImputationStrategy extends ImputationStrategy {
  override def impute(data: Array[Byte]): Array[Byte] = data.map(Missing.replaceNA(_, 0.toByte))
}

case class ModeImputationStrategy(noLevels: Int) extends ImputationStrategy {

  require(noLevels > 0)

  override def impute(data: Array[Byte]): Array[Byte] = {
    val counters = Array.ofDim[Int](noLevels)
    for (i <- data.indices) {
      if (Missing.isNotNA(data(i))) {
        counters(data(i)) += 1
      }
    }
    val modeValue: Byte = counters.indices.maxBy(counters).toByte
    data.map(Missing.replaceNA(_, modeValue))
  }
}
