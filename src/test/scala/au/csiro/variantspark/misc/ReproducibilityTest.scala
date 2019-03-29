package au.csiro.variantspark.misc

import org.apache.spark.sql.functions._
import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Ignore
import org.junit.Assert._
import au.csiro.variantspark.api._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * This test needs to be run standalone as it need a different spark context than other tests. 
 */
class ReproducibilityTest extends SparkTest {
  
  override implicit lazy val spark = SparkSession.builder.config(new SparkConf(false)).appName("test").master("local[*]").getOrCreate()
  
  @Ignore
  @Test
  def testReproducibleResults() {    
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importVCF("data/chr22_1000.vcf", 3)
    val label = vsContext.loadLabel("data/chr22-labels.csv", "22_16051249")
    val impAnalysis1 = features.importanceAnalysis(label, nTrees = 40, seed = Some(13L), mtryFraction = None, batchSize = 20)
    val topVariables1 = impAnalysis1.importantVariables(20)
    topVariables1.foreach(println)
    val impAnalysis2 = features.importanceAnalysis(label, nTrees = 40, seed = Some(13L), mtryFraction = None, batchSize = 20)
    val topVariables2 = impAnalysis2.importantVariables(20)
    topVariables2.foreach(println)
    topVariables1.zip(topVariables2).foreach{ p =>
      assertEquals(p._1, p._2)
    }
  }
}


