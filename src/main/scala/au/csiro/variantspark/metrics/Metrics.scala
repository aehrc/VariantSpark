package au.csiro.variantspark.metrics

object Metrics {

  def over2(n:Int) = n*(n-1)/2
  
  def adjustedRandIndex(c1:List[Int], c2:List[Int]): Double = {
    require(c1.size == c2.size)
    val c1PartitionNo = c1.max + 1
    val c2PartitionNo = c2.max + 1
    val n = c1.size
    val contingencyMatrix = Array.fill(c1PartitionNo)(Array.fill(c2PartitionNo)(0))
    Range(0,c1.size).foreach { i  => contingencyMatrix(c1(i))(c2(i)) +=1 }    
    val c1Bound = contingencyMatrix.map(_.sum)
    val c2Bound = Range(0,c2PartitionNo).map(j=> contingencyMatrix.map(_(j)).sum)
    val index:Int = contingencyMatrix.flatMap(_.map(over2)).sum
    val sumAi = c1Bound.map(over2).sum
    val sumBi = c2Bound.map(over2).sum
    (index.toDouble - sumAi*sumBi/over2(n).toDouble)/((sumAi+sumBi)/2.0 - sumAi*sumBi/over2(n).toDouble)
  }
  
  def accuracy(expected:Array[Int], predicted:Array[Int]):Double = {
    expected.toSeq.zip(predicted).filter(i => i._1 != i._2).size.toDouble/expected.length
  }
  
  def classificatoinError(expected:Array[Int], predicted:Array[Int]):Double = accuracy(expected, predicted)
}