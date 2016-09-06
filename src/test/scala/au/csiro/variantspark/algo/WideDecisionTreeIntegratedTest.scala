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
import au.csiro.variantspark.data.BoundedOrdinal

class WideDecisionTreeIntegratedTest extends SparkTest {
   
  implicit val fss = FileSystem.get(sc.hadoopConfiguration)
    //TODO (Should be moved to the test - but for some reason was null then ...)
  
  /**
   * This will try to classify CNAE-9 dataset (https://archive.ics.uci.edu/ml/datasets/CNAE-9)
   * using a full decision tree and compare the results to the classification done by R rpart.
   * Test data are produced by 'src/test/R/make_test_data.R' R script
   * 
   */
  def checkCNAE_9_Dataset(maxDepth:Int) { 
    val labelSource = new CsvLabelSource("data/CNAE-9-labels.csv", "category")
    val featureSource = new CsvFeatureSource(sc.textFile("data/CNAE-9-wide.csv"))
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val inputData = featureSource.features().map(_.toVector.values).cache()
    val nVars = inputData.count
    // max fife levels
    val dataType = BoundedOrdinal(5)
    val model = new WideDecisionTree(DecisionTreeParams(maxDepth = maxDepth)).run(inputData, dataType, labels)
    val prediction = model.predict(inputData)

    
    // check predictions
    val expected = CsvParser.parse(CsvFile("src/test/data/CNAE-9_R_predictions.csv")).withRowIndex(0).withColIndex(0)
          .firstCol(s"maxdepth_${maxDepth}").mapValues(CsvParser.parseInt).values.toSeq.toArray
    assertArrayEquals(expected, prediction)
 
 
    // check variable imporcances
    val expectedImportances = CsvParser.parse(CsvFile("src/test/data/CNAE-9_R_importance.csv")).withRowIndex(0).withColIndex(0)
          .firstCol(s"maxdepth_${maxDepth}").mapValues(CsvParser.parseDouble).values.toSeq.toArray

    val computedImportances= Array.fill(nVars.toInt)(0.0)
    model.variableImportanceAsFastMap.asScala.foreach { case (i,v) =>  computedImportances(i.toInt) = v }          
    assertArrayEquals(expectedImportances,computedImportances, 0.00001)
  }  
  
  
  @Test
  def testCNAE_9_DatasetWithMaxDepth4() { 
    checkCNAE_9_Dataset(4)
  }

// TODO: Check why this one fails
//  @Test
//  def testCNAE_9_DatasetWithMaxDepth15() { 
//    checkCNAE_9_Dataset(15)
//  }

  @Test
  def testCNAE_9_DatasetWithMaxDepth30() { 
    checkCNAE_9_Dataset(30)
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
    
    val model = new WideDecisionTree().run(data, BoundedOrdinal(3), labels)
    model.printout()
    
  }

}