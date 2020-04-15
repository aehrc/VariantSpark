package au.csiro.variantspark.data

import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class VectorData(valueAsVector: Vector) extends Data {
  def value: Any = valueAsVector
  def valueAsByteArray: Array[Byte] = valueAsVector.toArray.map(_.toByte)
  def valueAsIntArray: Array[Int] = valueAsVector.toArray.map(_.toInt)
  def valueAsStrings: List[String] = valueAsVector.toArray.map(_.toString).toList
  def size: Int = valueAsVector.size
  def at(i: Int): Double = valueAsVector(i)
}

case class IntArrayData(valueAsIntArray: Array[Int]) extends Data with Equals {
  def value: Any = valueAsIntArray
  def valueAsByteArray: Array[Byte] = valueAsIntArray.map(_.toByte)
  def valueAsVector: Vector = Vectors.dense(valueAsByteArray.map(_.toDouble))
  def valueAsStrings: List[String] = valueAsByteArray.map(_.toString).toList
  def size: Int = valueAsIntArray.length
  def at(i: Int): Double = valueAsIntArray(i).toDouble

  override def equals(other: Any): Boolean = {
    other match {
      case that: IntArrayData => valueAsIntArray.toSeq == that.valueAsIntArray.toSeq
      case _ => false
    }
  }

  override def hashCode(): Int = {
    valueAsIntArray.toSeq.hashCode
  }
}

case class ByteArrayData(valueAsByteArray: Array[Byte]) extends Data with Equals {
  def value: Any = valueAsByteArray
  def valueAsIntArray: Array[Int] = valueAsByteArray.map(_.toInt)
  def valueAsVector: Vector = Vectors.dense(valueAsByteArray.map(_.toDouble))
  def valueAsStrings: List[String] = valueAsByteArray.map(_.toString).toList
  def size: Int = valueAsByteArray.length
  def at(i: Int): Double = valueAsByteArray(i).toDouble

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteArrayData => valueAsByteArray.toSeq == that.valueAsByteArray.toSeq
      case _ => false
    }
  }

  override def hashCode(): Int = {
    valueAsByteArray.toSeq.hashCode
  }
}
