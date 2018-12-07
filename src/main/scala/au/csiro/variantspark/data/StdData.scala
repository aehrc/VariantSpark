package au.csiro.variantspark.data


import org.apache.spark.mllib.linalg.{Vector, Vectors}


case class VectorData(valueAsVector: Vector) extends Data {
  def value:Any =  valueAsVector
  def valueAsByteArray = valueAsVector.toArray.map(_.toByte)
  def valueAsStrings  = valueAsVector.toArray.map(_.toString).toList
  def size:Int = valueAsVector.size
  def at(i:Int):Double = valueAsVector(i)
}


case class ByteArrayData(valueAsByteArray: Array[Byte]) extends Data with Equals {

  def value:Any =  valueAsByteArray  
  def valueAsVector = Vectors.dense(valueAsByteArray.map(_.toDouble))
  def valueAsStrings  = valueAsByteArray.map(_.toString).toList
  def size:Int = valueAsByteArray.length
  def at(i:Int):Double = valueAsByteArray(i).toDouble
  
  
  override def equals(other: Any) = {
    other match {
      case that: ByteArrayData => valueAsByteArray.toSeq == that.valueAsByteArray.toSeq
      case _ => false
    }
  }

  override def hashCode() = {
    valueAsByteArray.toSeq.hashCode
  }  
}

