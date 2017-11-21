package au.csiro.variantspark

import au.csiro.variantspark.input.FeatureSource


/** Provides API for Variant Spark.
 *
 *  ==Overview==
 *  The main class to use is [[au.csiro.variantspark.VSContext]], as so
 *  {{{
 *  import au.csiro.variantspark.api._
 *  implicit val vsContext = VSContext(spark)
 *  val features = vsContext.importVCF("data/chr22_1000.vcf")
 *  val label = vsContext.loadLabel("data/chr22-labels.csv", "22_16050408")
 *  val impAnalysis = features.importanceAnalysis(label, nTrees = 100, seed = Some(13L))
 *  // retrieve top 20 variables as a Seq
 *  val top20Variables = impAnalysis.importantVariables(20)
 *  // or retrieve all of them as a DataFram
 *  val variableDF = impAnalysis.variableImportance
 *  }}}
 *  
 */
package object api {
  
  implicit def toAnalyticsFunctions(fs:FeatureSource):AnalyticsFunctions = {
    new AnalyticsFunctions(fs)
  }
}