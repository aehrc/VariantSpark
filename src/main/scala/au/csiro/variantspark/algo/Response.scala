package au.csiro.variantspark.algo

/**
  * The dependent variable passed into training. Sealed so that problem type
  * and response type must always be consistent.
  */
sealed trait ResponseVariable {
  def length: Int
}

/**
  * Response variable for classification problems.
  * @param labels integer class labels, one per sample (0-based)
  */
case class ClassificationResponse(labels: Array[Int]) extends ResponseVariable {
  def length: Int = labels.length
  def nLabels: Int = labels.max + 1
}

/**
  * Response variable for regression problems.
  * @param values continuous target values, one per sample
  */
case class RegressionResponse(values: Array[Double]) extends ResponseVariable {
  def length: Int = values.length
}

object ResponseVariable {
  implicit def apply(labels: Array[Int]): ResponseVariable = ClassificationResponse(labels)
  implicit def apply(values: Array[Double]): ResponseVariable = RegressionResponse(values)
}
