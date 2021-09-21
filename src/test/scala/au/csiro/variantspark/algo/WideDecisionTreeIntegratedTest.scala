package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.data.{BoundedOrdinalVariable, ContinuousVariable, VariableType}
import au.csiro.variantspark.input.{CsvFeatureSource, CsvLabelSource}
import au.csiro.variantspark.test.{SparkTest, TestCsvUtils}
import org.apache.hadoop.fs.FileSystem
import org.junit.Assert._
import org.junit.Test

class WideDecisionTreeIntegratedTest extends SparkTest {

  implicit val fss = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf = sc.hadoopConfiguration

  // TODO (Should be moved to the test - but for some reason was null then ...)

  /**
    * This will try to classify CNAE-9 dataset (https://archive.ics.uci.edu/ml/datasets/CNAE-9)
    * using a full decision tree and compare the results to the classification done by R rpart.
    * Test data are produced by 'src/test/R/make_test_data.R' R script
    *
    */
  def checkCNAE_9_Dataset(maxDepth: Int, dataType: VariableType = BoundedOrdinalVariable(5)) {
    val labelSource = new CsvLabelSource("data/CNAE-9-labels.csv", "category")
    val featureSource = new CsvFeatureSource(sc.textFile("data/CNAE-9-wide.csv"), dataType)
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val inputData = featureSource.features.zipWithIndex.cache()
    val nVars = inputData.count
    // max fife levels
    val model = new DecisionTree(DecisionTreeParams(maxDepth = maxDepth)).train(inputData, labels)
    val prediction = model.predict(inputData)

    // check predictions
    val expected = TestCsvUtils.readColumnToIntArray("src/test/data/CNAE-9_R_predictions.csv",
      s"maxdepth_${maxDepth}")
    assertArrayEquals(expected, prediction)

    // check variable importances
    val expectedImportances = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/CNAE-9_R_importance.csv", s"maxdepth_${maxDepth}")
    val computedImportances = Array.fill(nVars.toInt)(0.0)
    model.variableImportanceAsFastMap.asScala.foreach {
      case (i, v) => computedImportances(i.toInt) = v
    }
    assertArrayEquals(expectedImportances, computedImportances, 0.00001)
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
  def testCNAE_9_DatasetWithMaxDepth4_onContinous() {
    checkCNAE_9_Dataset(4, ContinuousVariable)
  }
  @Test
  def testCNAE_9_DatasetWithMaxDepth30_onContinous() {
    checkCNAE_9_Dataset(30, ContinuousVariable)
  }
}
