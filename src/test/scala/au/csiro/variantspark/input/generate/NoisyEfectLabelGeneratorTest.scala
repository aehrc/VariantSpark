package au.csiro.variantspark.input.generate


import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.test.SparkTest

import breeze.stats.meanAndVariance

class NoisyEfectLabelGeneratorTest extends SparkTest {
  
  @Test
  def testResponseGenertion_var_0_2_prec_0_75() {
    
    val freatureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator  = new NoisyEfectLabelGenerator(freatureGenerator)(1, Map("v_0"-> 1.0, "v_1"-> 0.5, "v_2"-> 0.25), fractionVarianceExplained = 0.2, classThresholdPrecentile = 0.75)
    val classes = labelGenerator.getLabels(freatureGenerator.sampleNames)
    // we wouild expect 75% of samples in class 0
    assertEquals(0.75, classes.count(_ == 0).toDouble/classes.size, 0.01)
    
    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.2, baseVariance/totalVariance, 0.02)
    
  }
  
  
   @Test
  def testResponseGenertion_var_0_5_prec_0_50() {
    
    val freatureGenerator = OrdinalFeatureGenerator(3, 1000, 500)
    val labelGenerator  = new NoisyEfectLabelGenerator(freatureGenerator)(1, Map("v_0"-> 1.0, "v_1"-> 0.5, "v_2"-> 0.25), fractionVarianceExplained = 0.5, classThresholdPrecentile = 0.5)
    val classes = labelGenerator.getLabels(freatureGenerator.sampleNames)
    // we wouild expect 75% of samples in class 0
    assertEquals(0.5, classes.count(_ == 0).toDouble/classes.size, 0.01)

    val baseVariance = meanAndVariance(labelGenerator.baseContinuousResponse).variance
    val totalVariance = meanAndVariance(labelGenerator.noisyContinuousResponse).variance
    assertEquals(0.5, baseVariance/totalVariance, 0.02)
  }
}