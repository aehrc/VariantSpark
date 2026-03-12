package au.csiro.variantspark.input

// Base trait - generic response source
trait ResponseSource[T] {
  def getResponses(sampleIds: Seq[String]): Array[T]
}

trait LabelSource extends ResponseSource[Int]
trait ValueSource extends ResponseSource[Double]
