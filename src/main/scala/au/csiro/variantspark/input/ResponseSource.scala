package au.csiro.variantspark.input

import scala.reflect.ClassTag

// Base trait - generic response source
trait ResponseSource {
  def getResponses[T: ClassTag](sampleIds: Seq[String], convert: String => T): Array[T]
}
