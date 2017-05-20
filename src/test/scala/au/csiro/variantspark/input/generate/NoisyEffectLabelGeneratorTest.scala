package au.csiro.variantspark.input.generate


import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.test.SparkTest

import breeze.stats.meanAndVariance
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.Feature
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import breeze.linalg.DenseVector

class TestFeatureGenerator(val samples: Seq[Feature])(implicit sc: SparkContext) extends FeatureSource {
  def features(): RDD[Feature] = sc.parallelize(samples)

  def sampleNames: List[String] = Range(0, samples.head.values.length).map("s_" + _).toList

}


class NoisyEffectLabelGeneratorTest extends SparkTest {

  @Test
  def testResponseGeneration_var_0_2_prec_0_75() {
    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEfectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25), fractionVarianceExplained = 0.2, classThresholdPrecentile = 0.75)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    // we would expect 75% of samples in class 0
    assertEquals(0.75, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.2, baseVariance / totalVariance, 0.02)
  }


  @Test
  def testMultiplicativeResponseGeneration_var_0_2_prec_0_75() {
    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEfectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25),
      fractionVarianceExplained = 0.2, classThresholdPrecentile = 0.75, multiplicative = true)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    // we would expect 75% of samples in class 0
    assertEquals(0.75, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.2, baseVariance / totalVariance, 0.02)
  }

  @Test
  def testAdditiveEffectCorrectness() {
    val featureGenerator = new TestFeatureGenerator(List(
      Feature("v_0", Array[Byte](0, 1, 2, 0)),
      Feature("v_1", Array[Byte](0, 1, 2, 1)),
      Feature("v_2", Array[Byte](0, 1, 2, 2)),
      Feature("v_3", Array[Byte](2, 2, 2, 2))
    ))
    val labelGenerator = new NoisyEfectLabelGenerator(featureGenerator)(1, Map("v_0" -> 0.1, "v_1" -> 0.5, "v_2" -> 2.0),
      fractionVarianceExplained = 0.2, classThresholdPrecentile = 0.75, multiplicative = false)

    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(DenseVector[Double](-2.6, 0, 2.6, 1.9), labelGenerator.baseContinuousResponse)
  }

  @Test
  def testMultiplicativeEffectCorrectness() {
    val featureGenerator = new TestFeatureGenerator(List(
      Feature("v_0", Array[Byte](0, 1, 2, 0)),
      Feature("v_1", Array[Byte](0, 1, 2, 1)),
      Feature("v_2", Array[Byte](0, 1, 2, 2)),
      Feature("v_3", Array[Byte](2, 2, 2, 2))
    ))
    val labelGenerator = new NoisyEfectLabelGenerator(featureGenerator)(1, Map("v_0" -> 0.1, "v_1" -> 0.5, "v_2" -> 2.0),
      fractionVarianceExplained = 0.2, classThresholdPrecentile = 0.75, multiplicative = true)

    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(DenseVector[Double](-0.1, 1.0, 0.1, -0.2), labelGenerator.baseContinuousResponse)
  }


  @Test
  def testResponseGeneration_var_0_5_prec_0_50() {
    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEfectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25), fractionVarianceExplained = 0.5, classThresholdPrecentile = 0.5)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    // we wouild expect 75% of samples in class 0
    assertEquals(0.5, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.5, baseVariance / totalVariance, 0.02)
  }
}