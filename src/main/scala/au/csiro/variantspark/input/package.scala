package au.csiro.variantspark

import org.apache.spark.mllib.linalg.{Vector, Vectors}


package object input {
    
  implicit object CanRepresentVector extends CanRepresent[Vector] with Serializable {
    def from(label:String, l:List[String]) = VectorFeature(label, Vectors.dense(l.map(_.toDouble).toArray))
    def from(label:String,  l:Array[Byte]) = VectorFeature(label, Vectors.dense(l.map(_.toDouble).toArray))
    def toListOfStrings(v:Vector):List[String]  = v.toArray.toList.map(_.toString)
  }

  implicit object CanRepresentByteArray extends CanRepresent[Array[Byte]] with Serializable {
    def from(label:String, l:List[String]) = ByteArrayFeature(label, l.map(_.toByte).toArray)
    def from(label:String, l:Array[Byte]) = ByteArrayFeature(label, l)
    def toListOfStrings(v:Array[Byte]):List[String]  = v.toList.map(_.toString)
  } 
}