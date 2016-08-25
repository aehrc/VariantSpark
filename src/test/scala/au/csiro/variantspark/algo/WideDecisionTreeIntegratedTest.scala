package au.csiro.variantspark.algo

import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.test.SparkTest
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.input.CsvLabelSource
import org.apache.hadoop.fs.FileSystem

import org.saddle.io._
import org.saddle._

class WideDecisionTreeIntegratedTest extends SparkTest {
  
  /**
   * This will try to classify CNAE-9 dataset (https://archive.ics.uci.edu/ml/datasets/CNAE-9)
   * using a full decision tree and compare the results to the classification done by the sklean implementation.
   * Test data are produced by 'src/test/python/make_test_data.py' python script
   * 
   */
  @Test
  def testCNAE_9_Dataset() {
   
    //TODO (Should be moved to the test - but for some reason was null then ...)
    implicit val fss = FileSystem.get(sc.hadoopConfiguration)
    val labelSource = new CsvLabelSource("data/CNAE-9-labels.csv", "category")
    val featureSource = new CsvFeatureSource("data/CNAE-9-wide.csv")
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val inputData = featureSource.features().map(_.toVector.values).cache()
    val model = new WideDecisionTree().run(inputData, labels)
    val prediction = model.predict(inputData)

    val predcition_df = CsvParser.parse(CsvFile("src/test/data/CNAE-9-predicted.csv")).withRowIndex(0).withColIndex(0)
    val expexted = predcition_df.firstCol("predicted").mapValues(CsvParser.parseInt).values.toSeq.toArray
    
    assertArrayEquals(expexted, prediction)
    // now load a bunch of csv file

    /**
     * Unfortunatelly this does not work as expected
     * because the trees in python have random element (variable pertmuation) so 
     * every time I run it I am getting different results
     * TODO: try some other referecne implementation  
     
    val tree_df = CsvParser.parse(CsvFile("src/test/data/CNAE-9-tree.csv")).withRowIndex(0).withColIndex(0)
    val feature = tree_df.firstCol("feature").mapValues(CsvParser.parseLong).values.toSeq.filter(_ >=0).toArray
    val impurity = tree_df.firstCol("impurity").mapValues(CsvParser.parseDouble).values.toSeq.toArray
    val threshold = tree_df.firstCol("threshold").mapValues(CsvParser.parseDouble).values.toSeq.filter(_ >=0.0).map(_.toInt.toDouble).toArray
    assertArrayEquals(feature, model.variables.toArray)
    assertArrayEquals(impurity, model.impurity.toArray, 0.0001)
    assertArrayEquals(threshold, model.threshods.toArray, 0.0001)
		*/
  }
  
  @Test
  def testSplitsCorrectlyForFullData() {
    
    val data = sc.parallelize(List.fill(4)(Vectors.dense(0.0, 1.0, 2.0)))
    
    val decisionTreeModel = new WideDecisionTreeModel( 
      SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 1.0,  splitVariableIndex = 1L,  splitPoint = 1.0, impurityReduction = 0.0, 
          left = SplitNode(majorityLabel = 0, size = 4, nodeImpurity = 0.4,  splitVariableIndex = 2L,  splitPoint = 0.0, impurityReduction = 0.0,
            left = LeafNode(0 ,3, 0.2),
            right = LeafNode(1 ,1, 0.1)
          ),
          right = SplitNode(majorityLabel = 0, size = 6, nodeImpurity = 0.6,  splitVariableIndex = 2L,  splitPoint = 0.0, impurityReduction = 0.0,
            left = LeafNode(2 ,2, 0.1),
            right = LeafNode(3 ,4, 0.2)
          )
       )
    )
    
    val labels = decisionTreeModel.predict(data)
    println(labels.toList)
    
    val model = new WideDecisionTree().run(data, labels)
    model.printout()
    
  }

}