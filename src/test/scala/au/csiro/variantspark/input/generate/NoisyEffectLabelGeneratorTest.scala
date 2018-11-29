package au.csiro.variantspark.input.generate


import au.csiro.variantspark.input.{FeatureSource}
import au.csiro.variantspark.data.ByteArrayFeature
import au.csiro.variantspark.test.SparkTest
import breeze.linalg.DenseVector
import breeze.stats.meanAndVariance
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.input._
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.FeatureBuilder

class TestFeatureGenerator(val samples: Seq[ByteArrayFeature])(implicit sc: SparkContext) extends FeatureSource {
  def features:RDD[Feature] = sc.parallelize(samples)
  def sampleNames: List[String] = Range(0, samples.head.size).map("s_" + _).toList

}


class NoisyEffectLabelGeneratorTest extends SparkTest {

  @Test
  def testResponseGeneration_var_0_2_prec_0_75() {

    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEffectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25), fractionVarianceExplained = 0.2, classThresholdPercentile = 0.75)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(0.75, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.2, baseVariance / totalVariance, 0.02)
  }


  @Test
  def testMultiplicativeResponseGeneration_var_0_2_prec_0_75() {

    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEffectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25),
      fractionVarianceExplained = 0.2, classThresholdPercentile = 0.75, multiplicative = true)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(0.75, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.2, baseVariance / totalVariance, 0.02)
  }

  @Test
  def testAdditiveEffectCorrectness() {
    val featureGenerator = new TestFeatureGenerator(List(
      ByteArrayFeature("v_0", Array[Byte](0, 1, 2, 0)),
      ByteArrayFeature("v_1", Array[Byte](0, 1, 2, 1)),
      ByteArrayFeature("v_2", Array[Byte](0, 1, 2, 2)),
      ByteArrayFeature("v_3", Array[Byte](2, 2, 2, 2))
    ))
    val labelGenerator = new NoisyEffectLabelGenerator(featureGenerator)(1, Map("v_0" -> 0.1, "v_1" -> 0.5, "v_2" -> 2.0),
      fractionVarianceExplained = 0.2, classThresholdPercentile = 0.75, multiplicative = false)

    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(DenseVector[Double](-2.6, 0, 2.6, 1.9), labelGenerator.baseContinuousResponse)
  }

  @Test
  def testMultiplicativeEffectCorrectness() {
    val featureGenerator = new TestFeatureGenerator(List(
      ByteArrayFeature("v_0", Array[Byte](0, 1, 2, 0)),
      ByteArrayFeature("v_1", Array[Byte](0, 1, 2, 1)),
      ByteArrayFeature("v_2", Array[Byte](0, 1, 2, 2)),
      ByteArrayFeature("v_3", Array[Byte](2, 2, 2, 2))
    ))
    val labelGenerator = new NoisyEffectLabelGenerator(featureGenerator)(1, Map("v_0" -> 0.1, "v_1" -> 0.5, "v_2" -> 2.0),
      fractionVarianceExplained = 0.2, classThresholdPercentile = 0.75, multiplicative = true)

    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(DenseVector[Double](-0.1, 1.0, 0.1, -0.2), labelGenerator.baseContinuousResponse)
  }


  @Test
  def testResponseGeneration_var_0_5_prec_0_50() {
    val featureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator = new NoisyEffectLabelGenerator(featureGenerator)(1, Map("v_0" -> 1.0, "v_1" -> 0.5, "v_2" -> 0.25), fractionVarianceExplained = 0.5, classThresholdPercentile = 0.5)
    val classes = labelGenerator.getLabels(featureGenerator.sampleNames)
    assertEquals(0.5, classes.count(_ == 0).toDouble / classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.5, baseVariance / totalVariance, 0.02)
  }
}