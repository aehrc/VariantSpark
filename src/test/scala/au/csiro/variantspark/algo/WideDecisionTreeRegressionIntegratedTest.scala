package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.data.{BoundedOrdinalVariable, ContinuousVariable, VariableType}
import au.csiro.variantspark.input.{CsvFeatureSource, CsvResponseSource}
import au.csiro.variantspark.test.{SparkTest, TestCsvUtils}
import org.apache.hadoop.fs.FileSystem
import org.junit.Assert._
import org.junit.Test

class WideDecisionTreeRegressionIntegratedTest extends SparkTest {

  implicit val fss = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf = sc.hadoopConfiguration

  // TODO (Should be moved to the test - but for some reason was null then ...)

  /**
    * This will try to regress CNAE-9 dataset (https://archive.ics.uci.edu/ml/datasets/CNAE-9)
    * using a full decision tree and compare the results to the regression done by R rpart.
    * Test data are produced by 'src/test/R/make_test_data.R' R script
    *
    */
  def checkCNAE_9_Dataset(maxDepth: Int, dataType: VariableType = BoundedOrdinalVariable(5),
      minRelativeImprovementFraction: Double = 1e-8, stabilityMultiplier: Double = 1e4): Unit = {
    val responseSource = new CsvResponseSource("data/CNAE-9-labels.csv", "category")
    val featureSource = new CsvFeatureSource(sc.textFile("data/CNAE-9-wide.csv"), dataType)
    val responses = responseSource.getResponses(featureSource.sampleNames, _.toDouble)
    val inputData = featureSource.features.zipWithIndex.cache()
    val nVars = inputData.count
    // max fife levels
    val model =
      new DecisionTree(DecisionTreeParams(problemType = Regression, maxDepth = maxDepth,
          minRelativeImprovementFraction = minRelativeImprovementFraction,
          stabilityMultiplier = stabilityMultiplier))
        .train(inputData, responses)
    val prediction = model.predict(inputData)

    // check predictions
    val expected = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/CNAE-9_R_predictions_regression.csv", s"maxdepth_${maxDepth}")
    assertArrayEquals(expected, prediction.map(_.asInstanceOf[Double]), 0.00001)

    // check variable importances
    val expectedImportances = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/CNAE-9_R_importance_regression.csv", s"maxdepth_${maxDepth}")
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

  @Test
  def testCNAE_9_DatasetWithMaxDepth15() {
    checkCNAE_9_Dataset(15)
  }

  @Test
  def testCNAE_9_DatasetWithMaxDepth4_onContinous() {
    checkCNAE_9_Dataset(4, ContinuousVariable)
  }

  @Test
  def testCNAE_9_DatasetWithMaxDepth15_onContinous() {
    checkCNAE_9_Dataset(15, ContinuousVariable)
  }
}
