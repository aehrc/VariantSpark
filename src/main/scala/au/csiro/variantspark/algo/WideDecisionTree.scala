package au.csiro.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.asScalaSet
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import scala.collection.mutable.MutableList
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.mllib.linalg.Vector
import au.csiro.pbdava.ssparkle.spark.SparkUtils._
import au.csiro.variantspark.metrics.Gini
import au.csiro.variantspark.utils.Sample
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.utils.FactorVariable
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator
import au.csiro.pbdava.ssparkle.common.utils.Prof
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.BoundedOrdinal

case class SubsetInfo(indices:Array[Int], impurity:Double, majorityLabel:Int) {
  def this(indices:Array[Int], impurity:Double, labels:Array[Int], nLabels:Int)  {
    this(indices, impurity, FactorVariable.labelMode(indices, labels, nLabels))
  }
  def lenght = indices.length
  override def toString(): String = s"SubsetInfo(${indices.toList},${impurity}, ${majorityLabel})"
}

case class SplitInfo(val splitPoint:Int, val gini:Double,  val leftGini:Double, val rightGini:Double)

case class VarSplitInfo(val variableIndex: Long, val splitPoint:Int, val gini:Double, val leftGini:Double, val rightGini:Double) {
  
  def split(splitVarData:Map[Long, Vector], labels:Array[Int], nCategories:Int)(subset:SubsetInfo):List[SubsetInfo] = {
    List(
        new SubsetInfo(subset.indices.filter(splitVarData(variableIndex)(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(splitVarData(variableIndex)(_) > splitPoint), rightGini, labels, nCategories)
    )
  }

  def split(data:Vector, labels:Array[Int], nCategories:Int)(subset:SubsetInfo):List[SubsetInfo] = {
    List(
        new SubsetInfo(subset.indices.filter(data(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(data(_) > splitPoint), rightGini, labels, nCategories)
    )
  }
}

case class VariableSplitter(val dataType: VariableType, val labels:Array[Int], mTryFactor:Double=1.0) extends Logging  with Prof {
  
  val nCategories = labels.max + 1
  
  def findSplits(data:Vector, splits:Array[SubsetInfo])(implicit rng:RandomGenerator = new JDKRandomGenerator()):Array[SplitInfo] = {
    val splitter = dataType match {
      case BoundedOrdinal(nLevels) => new JConfusionClassificationSplitter(labels, nCategories, nLevels)
      case _ => throw new RuntimeException(s"Data type ${dataType} not supported")
    }
    // now I can filter out splits that do not improve gini
    splits.map { subsetInfo =>  
      if (rng.nextDouble() <= mTryFactor) { 
        val splitInfo = splitter.findSplit(data.toArray, subsetInfo.indices)
        if (splitInfo != null && splitInfo.gini < subsetInfo.impurity) splitInfo else null
      } else null 
    }
  }
  
  def findSplitsForVars(varData:Iterator[(Vector, Long)], splits:Array[SubsetInfo])(implicit rng:RandomGenerator = new JDKRandomGenerator()):Iterator[Array[VarSplitInfo]] = {
    profIt("Local: splitting") {
      //println("Splits: "  + splits.toList)
      val result = varData
        .map(vi => findSplits(vi._1, splits).map(si => if (si != null) VarSplitInfo(vi._2, si.splitPoint, si.gini, si.leftGini, si.rightGini) else null).toArray)
        .foldLeft(Array.fill[VarSplitInfo](splits.length)(null))(WideDecisionTree.merge)
  
      // the fold above can be done by spark 
      // but this help to optimize for performance
      Some(result).toIterator
    }
  }
  
  def buildSubsplits(varData:Iterator[(Vector, Long)], splits:Array[Array[Int]])  = {
    
  }
  
}


/**
 * So what are the stopping conditions
 * - tree depth
 * - maximum number of nodes (which makes sense now becasue trees as build in the wide first manner)
 * - the minimum size of a node (only split nodes if they have more nodes than min)
 * - the node is pure (that is gini is 0) in which case there is not need to split
 * - the split does not improve gini
 * - the split will result in nodes that are too small???
 */


object WideDecisionTree extends Logging  with Prof {
  
  /**
   * This would take a variable information (index and data) 
   * labels and the indexed representation of the current splits 
   * and the split range to process
   * 
   * @param data  - the values of the variable for each sample
   * @param labels - the labels (categories) for each sample
   * @param splits - the indexed representation of the current splits
   * @param splitRange - the range of splits to consider
   * @param mTryFraction - the fraction of splits to apply this variable to
   */  
  // but the whole thing is essentially an aggrefation on a map trying to find the best variable for each of the splits
  // in the list (they represents best splits a tree level)
  
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {
    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else if (s2.gini < s1.gini) s2 else if (s1.variableIndex < s2.variableIndex) s1 else s2
    }
    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
  
  def findSplitsForVariable(br_splits:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter])
      (varData:Iterator[(Vector, Long)])  =  br_splitter.value.findSplitsForVars(varData, br_splits.value)(new XorShift1024StarRandomGenerator())
  
      
  def xxxx(indexedData: RDD[(Vector, Long)], bestSplits:Array[VarSplitInfo], br_subsets:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter]) = {
          // TODO: Perhaps do not need to broadcast empty values
      val xxx = withBrodcast(indexedData)(bestSplits){ br_bestSplits =>       
        // this becomes really complex will try to simplify it later 
        indexedData.mapPartitions { it =>               
          val usefulSubsetSplitAndIndex = br_subsets.value.zip(br_bestSplits.value).filter(_._2 != null).zipWithIndex.toList
          val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
          it.flatMap { case (v,i) => 
            // esentially create all splits for this variable
            val subsetPairs = splitByVarIndex.lift(i).map { splits => 
              splits.map { case ((subsetInfo, splitInfo), si) =>
                (splitInfo.split(v, br_splitter.value.labels, br_splitter.value.nCategories)(subsetInfo).toArray, si)
              }
            }
            subsetPairs.getOrElse(Nil)
          }
        }.collect()
      }

      //println(xxx.toList)
      val aNextLevelSplits = Array.fill[SubsetInfo](xxx.length*2)(null)
      xxx.foreach { case (l,i) => 
          aNextLevelSplits(2*i) = l(0)
          aNextLevelSplits(2*i + 1) = l(1)
      }
 			//println( aNextLevelSplits.toList)
      aNextLevelSplits.toList
  }
      
  def findBestSplits(indexedData: RDD[(Vector, Long)], subsetsToSplit:List[SubsetInfo], br_splitter:Broadcast[VariableSplitter]) =  {
      profIt("REM:findBestSplits") { 
        val subsetsToSplitAsIndices = subsetsToSplit.toArray
        withBrodcast(indexedData)(subsetsToSplitAsIndices){ br_splits => 
          
          val bestSplits = indexedData
            .mapPartitions(WideDecisionTree.findSplitsForVariable(br_splits, br_splitter))
            .fold(Array.fill(subsetsToSplitAsIndices.length)(null))(WideDecisionTree.merge)
          
          // now with the same broadcast
          (bestSplits, xxxx(indexedData, bestSplits, br_splits, br_splitter))
        }
      }
  }
}

abstract class DecisionTreeNode(val majorityLabel: Int, val size: Int, val nodeImpurity: Double) {
  def isLeaf:Boolean
  
  def printout(level: Int) 
  def impurityContribution:Double = nodeImpurity * size
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int
  def toStream:Stream[DecisionTreeNode]
  def splitsToStream:Stream[SplitNode]  = toStream.filter(!_.isLeaf).asInstanceOf[Stream[SplitNode]]
  def leafsToStream:Stream[LeafNode]  = toStream.filter(_.isLeaf).asInstanceOf[Stream[LeafNode]]
}

case class LeafNode(override val majorityLabel: Int,override val  size: Int,override val  nodeImpurity: Double) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {
  val isLeaf = true

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "leaf"
    println(s"${nodeType}[${majorityLabel}, ${size}, ${nodeImpurity}]")
  }
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int = majorityLabel
  def toStream:Stream[DecisionTreeNode] = this #:: Stream.empty
}

case class SplitNode(override val majorityLabel: Int, override val size: Int,override val  nodeImpurity: Double, splitVariableIndex: Long, splitPoint: Double,
    impurityReduction: Double,left: DecisionTreeNode, right: DecisionTreeNode) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {
  
  val isLeaf = false
  
  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "split"
    println(s"${nodeType}[${splitVariableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}, ${nodeImpurity}]")
    left.printout(level + 1)
    right.printout(level + 1)      
  }
  
  // TODO (CHECK): Not sure but this can be the same as impurity reduction
  def impurityDelta  = impurityContribution - (left.impurityContribution + right.impurityContribution)
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int = if (variables(splitVariableIndex)(sampleIndex) <= splitPoint) left.predict(variables)(sampleIndex) else right.predict(variables)(sampleIndex)
  def toStream:Stream[DecisionTreeNode] = this #:: left.toStream #::: right.toStream
}

class WideDecisionTreeModel(val rootNode: DecisionTreeNode) extends PredictiveModelWithImportance with  Logging {
  
  def splitVariableIndexes = rootNode.splitsToStream.map(_.splitVariableIndex).toSet
  
  def predictIndexed(indexedData: RDD[(Vector,Long)]): Array[Int] = {
    val treeVariableData =  indexedData.collectAtIndexes(splitVariableIndexes)
    Range(0, indexedData.size).map(rootNode.predict(treeVariableData)).toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = rootNode.splitsToStream.
    foldLeft(new Long2DoubleOpenHashMap()){ case (m, splitNode) => 
      m.increment(splitNode.splitVariableIndex, splitNode.impurityDelta)
    }
  
  def impurity = rootNode.toStream.map(_.nodeImpurity).toList
  def variables = rootNode.splitsToStream.map(_.splitVariableIndex).toList
  def threshods = rootNode.splitsToStream.map(_.splitPoint).toList
}

case class DecisionTreeParams(val maxDepth:Int = Int.MaxValue, val minNodeSize:Int = 1)


class WideDecisionTree(val params: DecisionTreeParams = DecisionTreeParams()) extends Logging with Prof {
  def run(data: RDD[Vector], dataType: VariableType, labels: Array[Int]): WideDecisionTreeModel = run(data.zipWithIndex(), dataType, labels, 1.0, Sample.all(data.first().size))
  def run(data: RDD[(Vector, Long)], dataType: VariableType,  labels: Array[Int], nvarFraction: Double, sample:Sample): WideDecisionTreeModel = {
    // TODO: not sure while this is need
    val dataSize = data.count()
    //TODO (OPTIMIZE): Perhpas is't better to use unique indexes and weights
    val currentSet =  sample.indexesIn.toArray
    val splitter = VariableSplitter(dataType, labels, nvarFraction)
    val br_splitter = data.context.broadcast(splitter)
    val (totalGini, totalLabel) = Gini.giniImpurity(currentSet, labels, splitter.nCategories)
    val rootNode = buildSplit(data, List(SubsetInfo(currentSet, totalGini, totalLabel)), br_splitter, 0)
    br_splitter.destroy()    
    new WideDecisionTreeModel(rootNode.head)
  }

  /**
   * Trains all the trees for specified samples at the same time
   */
  def batchTrain(data: RDD[(Vector, Long)], dataType: VariableType, labels: Array[Int], nvarFraction: Double, sample:Seq[Sample]): Seq[WideDecisionTreeModel] = {
    // TODO: not sure while this is need
    //TODO (OPTIMIZE): Perhpas is't better to use unique indexes and weights
    val splitter = VariableSplitter(dataType, labels, nvarFraction)
    val br_splitter = data.context.broadcast(splitter)
    val subsets = sample.map { s => 
      val currentSet =  s.indexesIn.toArray
      val (totalGini, totalLabel) = Gini.giniImpurity(currentSet, labels, splitter.nCategories)
      SubsetInfo(currentSet, totalGini, totalLabel)
    }.toList
    val rootNodes = buildSplit(data, subsets, br_splitter, 0)
    br_splitter.destroy()    
    rootNodes.map(new WideDecisionTreeModel(_))
  }
  
  /**
   * Builds (recursively) the decision tree level by level
   * TODO: Intrisinlgy noting is stopping me from building many trees as the same time using different initial splits ....
   * that should even work withou much modifications (and significantly limit the communication required
   */
  
  def buildSplit(indexedData: RDD[(Vector, Long)], subsets: List[SubsetInfo], br_splitter: Broadcast[VariableSplitter],treeLevel:Int): List[DecisionTreeNode] = {
      // for the current set find all candidate splits    
      logDebug(s"Building level ${treeLevel}, subsets: ${subsets}") 
       
      
      profReset()
      // pre-filter the subsets to check if they need further splitting
      // we also need to maintain the original index of the subset so that we can later 
      // join the next level tree nodes
      
      val subsetsToSplit = subsets.zipWithIndex.filter {case (si, _) =>
        si.lenght >= params.minNodeSize && treeLevel < params.maxDepth
      }
      logDebug(s"Splits to include: ${subsetsToSplit}") 
      
      //TODO: (OPTIMIZE) if there is not splits to calculate do not call compute splits etc.            
      //TODO: (OPTIMIZE) if I pass the subset info including impurity I can do post-filtering in workers
        
      val (bestSplits, nextLevelSubsets) = WideDecisionTree.findBestSplits(indexedData, subsetsToSplit.unzip._1.toList, br_splitter)
      logDebug(s"Best splits: ${bestSplits.toList}")
      profPoint("Best splits")
     
     logDebug(s"Next level splits: ${nextLevelSubsets}")
     profPoint("Splitting done")
     
     // compute the next level tree nodes (notice the recursive call)
     val nextLevelNodes = if (!nextLevelSubsets.isEmpty) buildSplit(indexedData, nextLevelSubsets, br_splitter, treeLevel + 1) else List()
     
     profPoint("Sublevesl done")

     val (usefulSplits, usefulSplitsIndices) = bestSplits.zip(subsetsToSplit.unzip._2).filter(_._1 != null).unzip
     // compute the indexes of splitted subsets against the original indexes
     val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap

     // I need to be able to find which nodes to use for splits
     // so I need say an Array that would tell me now which nodes were actually passed to split and what their index vas
     // at this stage we wouild know exactly what we need
     val result = subsets.zipWithIndex.map({case (subset, i) => 
       subsetIndexToSplitIndexMap.get(i).map({splitIndex => 
         val split = usefulSplits(splitIndex)
         SplitNode(subset.majorityLabel, subset.lenght,  subset.impurity
             ,split.variableIndex, split.splitPoint, subset.impurity - split.gini,
             nextLevelNodes(2*splitIndex), nextLevelNodes(2*splitIndex+1))})
         .getOrElse(LeafNode(subset.majorityLabel, subset.lenght,  subset.impurity))
     }).toList
 
     profPoint("Bindin  done")
     
     result 
  }
}




