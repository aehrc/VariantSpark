package au.csiro.variantspark.api


import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.sql.SQLContext

class ImportaceApiTest extends SparkTest {
  @Test
  def testCreateContext() {
    
    val sqlContext = new SQLContext(sc)    
    implicit val vsContext = VSContext(sqlContext)
    val fs = vsContext.featureSource("data/chr22_1000.vcf")
    println(fs.sampleNames)
    val ls  = vsContext.labelSource("data/chr22-labels.csv", "22_16050408")
    println(ls.getLabels(fs.sampleNames).toList)
    
    val importanceAnalysis = ImportanceAnalysis(fs, ls)
    val importanceDF = importanceAnalysis.variableImportance
    println("DF count")
    importanceDF.cache()
    println(importanceDF.count())
    importanceDF.registerTempTable("importance")
    sqlContext.sql("SELECT * FROM importance ORDER BY importance DESC limit 10").collect().foreach(println _)
    
  }
}