package au.csiro.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.asScalaSet
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.MutableList
import org.apache.spark.broadcast.Broadcast




object WideDecisionTree {

  def projectVector(indexSet: Set[Int], invert: Boolean = false)(v: Vector): Vector = {
    val a = v.toArray
    Vectors.dense((for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray)
  }

  def projectArray(indexSet: Set[Int], invert: Boolean = false)(a: Array[Int]): Array[Int] = {
    (for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray
  }

  def sqr(x: Double) = x * x

  def giniImpurity(counts: Array[Int]): Double = {
    val total = counts.sum.toDouble
    if (total == 0.0) 0.0 else 1 - counts.map(s => sqr(s / total)).sum
  }

  def giniImpurity(currentSet: Array[Int], labels: Array[Int], labelCount:Int):(Double,Int) = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    (giniImpurity(labelCounts), labelCounts.zipWithIndex.max._2)
  }
  
  class SparseVectorSequentialAccessor(val sv:SparseVector) {
    var index = 0
    def apply(i:Int):Double = {
      while(index < sv.indices.length && sv.indices(index) < i) { 
        index+=1
      }
      if (index < sv.indices.length && sv.indices(index) ==i ) sv.values(index) else 0.0
    }

  }
  
  def mergeForEeach(currentSet: Array[Int], v:Vector)(f:(Int,Double)=>Unit)  {
    // assumes currentSet is sorted
    // for dense vector
    v match {
      case dv:DenseVector => currentSet.foreach(i=> f(i,dv.values(i))) 
      case sv:SparseVector => {
        val accessor = new SparseVectorSequentialAccessor(sv)
        currentSet.foreach(i=> f(i,accessor(i)))
      }
    }
  }
  
  
  def findSplitInPartition(br_currentSet: Broadcast[Array[Int]], br_labels: Broadcast[Array[Int]], totalGini:Double)
      (partition:Iterator[(Vector,Long)]):Iterator[(Double, Long, Double, Array[Int], Array[Int])] = {
        
    val currentSet = br_currentSet.value
    val labels = br_labels.value
    
    //println("Processing partition")
    var bestData:Array[Double]= null
    var bestImpurity:Double = 2.0
    var bestVarIndex:Long = -1
    var bestSplit:Double = -1

    val labelsCount = labels.max + 1
    // early exit (if we haver already found a 0 impurity split there is not need to continue looking)
    while(bestImpurity > 0.0 &&  partition.hasNext) {
      val (vector,varIndex) = partition.next()
      val data = vector.toArray

      //println(s"Processin var ${varIndex}")


      val (impurity, splitPoint) = findSplit(currentSet: Array[Int], labels: Array[Int], data:Array[Double], labelsCount)
      if (impurity < bestImpurity) {
        bestData = data
        bestImpurity = impurity
        bestVarIndex = varIndex
        bestSplit = splitPoint
      }
    }
    val result:Option[(Double, Long, Double, Array[Int], Array[Int])] =
      if (bestData != null) Some((totalGini - bestImpurity,bestVarIndex, bestSplit, currentSet.filter(i =>  bestData(i)<= bestSplit),
        currentSet.filter(i =>  bestData(i)> bestSplit))) else None
    result.toIterator
  }
  
  def findSplit(currentSet: Array[Int], labels: Array[Int], data:Array[Double], labelsCount:Int): (Double, Double) = {

    // now this wouild be done fastest with a contingency table but with 3 values
    // lets to a simple approach first

    //assuming values are 0, 1, 2 there are two possible splits
    // left side split x<=i
    val possibleSplits = -5.0 to 5.0 by 0.1
    possibleSplits.map { s =>
      val leftCount = Array.fill(labelsCount)(0)
      val rightCount = Array.fill(labelsCount)(0)
      currentSet.foreach {i => if ( data(i)<= s) leftCount(labels(i)) += 1 else rightCount(labels(i)) += 1}
      val leftItems = leftCount.sum
      val rightItems = rightCount.sum
      val splitGini = (giniImpurity(leftCount) * leftItems.toDouble + giniImpurity(rightCount) * rightItems.toDouble) / (leftItems + rightItems)
      (splitGini, s)
    }.min
  }
}

case class DecisionTreeNode(variableIndex: Long, splitPoint: Double, majorityLabel: Int,
    impurityReduction: Double, nodeImpurity: Double, size: Int, left: DecisionTreeNode = null, right: DecisionTreeNode = null) {

  def isLeaf = (impurityReduction == 0)

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = if (isLeaf) "leaf" else "split"
    println(s"${nodeType}[${variableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}, ${nodeImpurity}]")
    if (!isLeaf) {
      left.printout(level + 1)
      right.printout(level + 1)
    }
  }

  def countImportance(accumulations: Long2DoubleOpenHashMap, totalSize:Int) {
    if (!isLeaf) {
      
      accumulations.addTo(variableIndex, (size*nodeImpurity - (left.size*left.nodeImpurity + right.size*right.nodeImpurity))/totalSize.toDouble)
      left.countImportance(accumulations, totalSize)
      right.countImportance(accumulations, totalSize)
    }
  }

}

class WideDecisionTreeModel(val rootNode: DecisionTreeNode) {

    def predict(data: RDD[Vector]): Array[Int] = predictIndexed(data.zipWithIndex())

    def predictIndexed(data: RDD[(Vector,Long)]): Array[Int] = {
    // this is a bit tricky but say lets' collect all the values neeed to resolve the thre

    //map the tree into a set of indexes

    def mapTrees(tree: DecisionTreeNode): List[Long] = {
      if (tree.impurityReduction > 0 && tree.left != null && tree.right != null) tree.variableIndex :: mapTrees(tree.left) ::: mapTrees(tree.right)
      else List()
    }

    val indexes = mapTrees(rootNode).toSet
    println(s"Collecting data: ${indexes.size}")
    val br_indexes = data.context.broadcast(indexes)
    // now collect values of selected index
    val pointsAsList = data.filter { case (v, i) => br_indexes.value.contains(i) }.collect()
    br_indexes.destroy()
    val points = pointsAsList.map(_.swap).toMap
    
    val tmp = Array.fill(data.first()._1.size)(rootNode)
    while (tmp.exists { x => x.impurityReduction > 0 }) {
      tmp.indices.foreach { i =>
        val tn = tmp(i)
        if (!tn.impurityReduction.isNaN() && tn.right != null && tn.left != null) {
          tmp(i) = if (points(tn.variableIndex)(i) <= tn.splitPoint) tn.left else tn.right
        }
      }
    }
    tmp.map(_.majorityLabel)
  }

  def printout() {
    rootNode.printout(0)
  }

  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = {
    val accumulations = new Long2DoubleOpenHashMap();
    rootNode.countImportance(accumulations, rootNode.size)
    accumulations
  }

  def variableImportance(): Map[Long, Double] = {
    variableImportanceAsFastMap.entrySet().map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap
  }

}

class WideDecisionTree {
  def run(data: RDD[Vector], labels: Array[Int]): WideDecisionTreeModel = run(data.zipWithIndex(), labels, Range(0, data.first().size).toArray, 0.3)
  def run(data: RDD[(Vector, Long)], labels: Array[Int], currentSet: Array[Int],nvarFraction: Double): WideDecisionTreeModel = {

    val c = data.count()

    val indexedData = data
    // what we need to do it so select variables for each      
    // sample a few variables.
    // indexes of elements included in current split
    // we need to sort the current set
    
    val br_labels = data.context.broadcast(labels)
    val tree = new WideDecisionTreeModel(buildSplit(indexedData, currentSet, br_labels, nvarFraction))
    br_labels.destroy()
    tree
  }

  def buildSplit(indexedData: RDD[(Vector, Long)], currentSet: Array[Int], br_labels: Broadcast[Array[Int]], nvarFraction: Double): DecisionTreeNode = {
    // for the current set find all candidate splits

    //println(s"Splitting: ${currentSet.toList}")
    
    
    val br_curretSet =  indexedData.context.broadcast(currentSet)
    
    val labelsCount = br_labels.value.max + 1
    val (totalGini, majorityLabel) = WideDecisionTree.giniImpurity(currentSet, br_labels.value, labelsCount)
    
    val (giniReduction, varIndex, split, leftSet, rightSet) = indexedData
      .sample(false, nvarFraction, (Math.random() * 10000).toLong) // sample the variables (should be sqrt(n)/n for classification)
      .mapPartitions(WideDecisionTree.findSplitInPartition(br_curretSet, br_labels, totalGini))
      .reduce((f1, f2) => if (f1._1 > f2._1) f1 else f2) // dumb way to use minimum

    // check if futher split is needed
    br_curretSet.destroy()
    //println("Gini reduction:" + giniReduction) 

    if (giniReduction > 0) {
      DecisionTreeNode(varIndex, split, majorityLabel, giniReduction, totalGini, currentSet.length,
          buildSplit(indexedData, leftSet, br_labels, nvarFraction),
          buildSplit(indexedData, rightSet, br_labels, nvarFraction))
    } else {
      DecisionTreeNode(varIndex, split, majorityLabel, giniReduction, totalGini, currentSet.length, null, null)
    }
  }
}




