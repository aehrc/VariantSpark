package au.csiro.variantspark.data
import org.apache.spark.mllib.linalg.{Vectors,Vector}

case class VectorFeature(label:String, variableType: VariableType,  valueAsVector: Vector) extends Feature {
  def value:Any =  valueAsVector
  def valueAsByteArray = valueAsVector.toArray.map(_.toByte)
  def valueAsStrings  = valueAsVector.toArray.map(_.toString).toList
  def size:Int = valueAsVector.size
  def at(i:Int):Double = valueAsVector(i)
}

object VectorFeature {
  def apply(label:String, values: Vector):VectorFeature = VectorFeature(label, ContinuousVariable, values)
}

case class ByteArrayFeature(label:String, variableType: VariableType, valueAsByteArray: Array[Byte]) extends Feature with Equals {
  require(label != null)
  require(valueAsByteArray != null)
  
  def value:Any =  valueAsByteArray  
  def valueAsVector = Vectors.dense(valueAsByteArray.map(_.toDouble))
  def valueAsStrings  = valueAsByteArray.map(_.toString).toList
  def size:Int = valueAsByteArray.length
  def at(i:Int):Double = valueAsByteArray(i).toDouble
  
  
  override def equals(other: Any) = {
    other match {
      case that: ByteArrayFeature => label == that.label && valueAsByteArray.toSeq == that.valueAsByteArray.toSeq
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + label.hashCode) + valueAsByteArray.toSeq.hashCode
  }  
}

object ByteArrayFeature {
  def apply(label:String, values: Array[Byte]):ByteArrayFeature = ByteArrayFeature(label, UnboundedOrdinalVariable, values)
}
