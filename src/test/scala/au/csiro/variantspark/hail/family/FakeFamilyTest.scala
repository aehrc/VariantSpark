package au.csiro.variantspark.hail.family

import org.junit.Test

import au.csiro.variantspark.hail.toVSGenericDatasetFunctions
import au.csiro.variantspark.hail.toVSHailContextFunctions
import au.csiro.variantspark.test.SparkTest
import is.hail.HailContext

class FakeFamilyTest extends SparkTest {

  @Test
  def testLoadPhased() {
    val hc = HailContext(sc)
    val vcf = hc.importVCFGenericEx("data/chr22_1000.vcf")
    println(vcf.count())
    vcf.exportVCFEx("target/genericPhasedO.vcf")
   }
   
}