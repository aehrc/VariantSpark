package au.csiro.variantspark
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

package object data {

  implicit case object VectorDataBuilder extends DataBuilder[Vector] {
    def from(l: List[String]): Data = {
      VectorData(Vectors.dense(l.map(_.toDouble).toArray))
    }
    def from(v: Vector): Data = {
      VectorData(v)
    }
    def defaultVariableType = ContinuousVariable
  }

  implicit case object ByteArrayDataBuilder
      extends DataBuilder[Array[Byte]] {
    def from(l: List[String]): Data = {
      ByteArrayData(l.map(_.toByte).toArray)
    }

    def from(v: Array[Byte]): Data = {
      ByteArrayData(v)
    }

    def defaultVariableType = BoundedOrdinalVariable(3)
  }

  implicit case object IntArrayDataBuilder
      extends DataBuilder[Array[Int]] {
    def from(l: List[String]): Data = {
      IntArrayData(l.map(_.toInt).toArray)
    }

    def from(v: Array[Int]): Data = {
      IntArrayData(v)
    }

    def defaultVariableType = OrdinalVariable
  }

  implicit def toFeatueConverter[V](v: RDD[V]): ToFeature[V] = new ToFeature[V](v)
}
