package au.csiro.variantspark.utils

case class FactorVariable(values: Array[Int], nCategories: Int,
    subset: Option[Array[Int]] = None) {

  def indices: Iterator[Int] =
    subset.map(_.toIterator).getOrElse(values.indices.toIterator)

  def counts: Array[Int] = {
    val labelCounts = Array.fill(nCategories)(0)
    indices.foreach(i => labelCounts(values(i)) += 1)
    labelCounts
  }

  def apply(indexes: Array[Int]): FactorVariable = {
    new FactorVariable(values, nCategories, Some(indexes))
  }
}

object FactorVariable {
  def apply(values: Array[Int]): FactorVariable = apply(values, values.max + 1)

  def labelMode(currentSet: Array[Int], labels: Array[Int], labelCount: Int): Int = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    labelCounts.zipWithIndex.maxBy(_._1)._2
  }
}
