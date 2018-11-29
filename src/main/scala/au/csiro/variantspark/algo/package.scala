package au.csiro.variantspark

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.CanSize
import au.csiro.variantspark.data.Feature

package object algo {
  
  implicit val canSplitVector = new CanSplitVector
  implicit val canSplitArrayOfBytes = new CanSplitArrayByte
  
  type WideDecisionTree = DecisionTree
  type WideDecisionTreeModel = DecisionTreeModel
  
  type WideRandomForest = RandomForest
  type WideRandomForestModel = RandomForestModel
  
  type ByteRandomForestModel = RandomForestModel
  type ByteRandomForest = RandomForest
   
  implicit case object CanSizeFeature extends CanSize[Feature] {
     def size(f:Feature):Int = f.size
  }
  
  implicit case object CanSplitSplittable extends CanSplit[Splittable] {
    def at(v: Splittable)(i: Int): Double = v.at(i)
    def size(v: Splittable): Int = v.size  
    def split(v: Splittable, splitter: ClassificationSplitter, indices: Array[Int]): SplitInfo = v.split(splitter, indices)
    def runtimeClass: Class[_] = classOf[Splittable]
  }
  
  
  
  implicit def toTreeFeatueRDD(rdd:RDD[TreeFeature]) = new TreeFeatueRDDFunction(rdd)

}