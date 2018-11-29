package au.csiro.variantspark.algo


import au.csiro.variantspark.test.SparkTest
import org.junit.Assert._
import org.junit.Test
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import au.csiro.variantspark.input.FeatureSource
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import au.csiro.variantspark.data.NumericalType
import au.csiro.variantspark.data.OrderedCategoricalType
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.utils.Sample
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.input.VectorFeature
import au.csiro.variantspark.input.ByteArrayFeature
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.input.ByteArrayFeatureBuilder
import au.csiro.variantspark.input.VectorFeatureBuilder
import au.csiro.variantspark.data.FeatureBuilder
import au.csiro.variantspark.test.TestFeatureSource


class PolymorphicDecisionTreeTest extends SparkTest {
  @Test
  def testTrainPolymorphicTree() {
    val genomicFeatureSource = new TestFeatureSource(Seq(
      ("gen_1", List("0", "0", "1", "2")),
      ("gen_2", List("0", "1", "0", "2"))          
    ), BoundedOrdinalVariable(3), ByteArrayFeatureBuilder)
    
    
    val otherFeatureSource = new TestFeatureSource(Seq(
      ("cont_1", List("0.2", "0.3", "1.4", "2.5")),
      ("cont_2", List("0.1", "1.3", "0.3", "2.6"))          
    ), ContinuousVariable, VectorFeatureBuilder)
    
    // load polymorphic features (possibly combining two homogenous sources)
    
    println(genomicFeatureSource.samples)
    println(otherFeatureSource.samples)
    
    
    val genomicFeatures = genomicFeatureSource.features
    genomicFeatures.foreach(println  _)
    val otherFeatures = otherFeatureSource.features
    otherFeatures.foreach(println  _)

    // variable type should be already here
    // that is the source should likely pre-determine what the variable types are:
    // or otherwise I need another layer of indirection to assign data typed later
    // but it might may sense to do it at data source level as for example for genomic data it makes sense to mark them as oridinals straight away
    val allFeatures = genomicFeatures.union(otherFeatures).zipWithIndex
    // not how to combine them into single typed feature set    
    allFeatures.foreach(println _)

    // not this needs to be simplified really to be just working on a single representation
    // with possibly typed constructors for some legacy cases
    // does not make sense really have per representation tree (does it?)
    
    val tree = new DecisionTree()
    tree.batchTrain(allFeatures, Array[Int](0,1,0,1), 1.0, List(Sample.all(allFeatures.first._1.size)))
    // so again limit the legacy cases for contruction only
    // and maybe can get rid of them in the future
    // build the interal tree representation
    // train the tree
  }
  
}