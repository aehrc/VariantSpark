package au.csiro.variantspark.hail.family


import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.variant._
import is.hail.utils._
import is.hail.expr._
import au.csiro.variantspark.hail._
import is.hail.variant.Variant
import is.hail.variant.GenotypeBuilder
import is.hail.variant.GenotypeStreamBuilder
import is.hail.variant.Genotype
import is.hail.annotations.Annotation
import is.hail.variant.GTPair
import is.hail.io.vcf.VCFSettings
import is.hail.io.vcf.LoadVCF
import au.csiro.variantspark.hail.variant.phased.PhasedGenericRecordReader
import is.hail.io.vcf.GenericRecordReader
import au.csiro.variantspark.hail.variant.io.ExportVCFEx


object FakeFamily {
  
  def createOffspringGeneric(v:Variant, genotypes:Iterable[Annotation]): Iterable[Annotation] = {
   
    // genotypes is iterable od row
    
    val parentGenotypes = genotypes.toList
    val offspringAndParent = Annotation(1) :: parentGenotypes
    offspringAndParent
  } 
  
  def createOffspring(v:Variant, genotypes:Iterable[Genotype]): Iterable[Genotype] = {
    val gsb = new GenotypeStreamBuilder(v.nAlleles, isLinearScale = false)
    genotypes.foreach { gt => 
       gsb += gt
    }
    val gb = new GenotypeBuilder(v.nAlleles, false)
    gb.setGT(0)
    gsb.write(gb)
    gsb.result() 
  }  
  
  
//    def importVCF(hc: HailContext, file: String, force: Boolean = false,
//    forceBGZ: Boolean = false,
//    headerFile: Option[String] = None,
//    nPartitions: Option[Int] = None,
//    dropSamples: Boolean = false,
//    storeGQ: Boolean = false,
//    ppAsPL: Boolean = false,
//    skipBadAD: Boolean = false): VariantDataset = {
//    importVCFs(hc, List(file), force, forceBGZ, headerFile, nPartitions, dropSamples,
//      storeGQ, ppAsPL, skipBadAD)
//  }
//
//  def importVCFs(hc:HailContext, files: Seq[String], force: Boolean = false,
//    forceBGZ: Boolean = false,
//    headerFile: Option[String] = None,
//    nPartitions: Option[Int] = None,
//    dropSamples: Boolean = false,
//    storeGQ: Boolean = false,
//    ppAsPL: Boolean = false,
//    skipBadAD: Boolean = false): VariantDataset = {
//
//    val inputs = LoadVCF.globAllVCFs(hc.hadoopConf.globAll(files), hc.hadoopConf, force || forceBGZ)
//
//    val header = headerFile.getOrElse(inputs.head)
//
//    val codecs = hc.sc.hadoopConfiguration.get("io.compression.codecs")
//
//    if (forceBGZ)
//      hc.hadoopConf.set("io.compression.codecs",
//        codecs.replaceAllLiterally("org.apache.hadoop.io.compress.GzipCodec", "is.hail.io.compress.BGzipCodecGZ"))
//
//    val settings = VCFSettings(storeGQ, dropSamples, ppAsPL, skipBadAD)
//    val reader = new PhasedGenericRecordReader() //(settings)
//    val vds = LoadVCF(hc, reader, header, inputs, nPartitions, dropSamples)
//
//    hc.hadoopConf.set("io.compression.codecs", codecs)
//
//    vds
//  }

   def importVCFGeneric(hc:HailContext,  file: String, force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String]): GenericDataset = {
    importVCFsGeneric(hc, List(file), force, forceBGZ, headerFile, nPartitions, dropSamples, callFields)
  }

  def importVCFsGeneric(hc:HailContext, files: Seq[String], force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String]): GenericDataset = {

    val inputs = LoadVCF.globAllVCFs(hc.hadoopConf.globAll(files), hc.hadoopConf, force || forceBGZ)

    val header = headerFile.getOrElse(inputs.head)

    val codecs = hc.sc.hadoopConfiguration.get("io.compression.codecs")

    if (forceBGZ)
      hc.hadoopConf.set("io.compression.codecs",
        codecs.replaceAllLiterally("org.apache.hadoop.io.compress.GzipCodec", "is.hail.io.compress.BGzipCodecGZ"))

    val reader = new PhasedGenericRecordReader(callFields)
    val gds = LoadVCF(hc, reader, header, inputs, nPartitions, dropSamples)

    hc.hadoopConf.set("io.compression.codecs", codecs)

    gds
  } 
}

class FakeFamilyTest extends SparkTest {

  @Test
  def testLoadPhased() {
    val hc = HailContext(sc)
    val vcf = FakeFamily.importVCFGeneric(hc, "data/chr22_1000.vcf")
    println(vcf.count())
    ExportVCFEx(vcf, "target/genericPhasedO.vcf")
   }
   

  @Test
  def testProduceOffspring() {
    val hc = HailContext(sc)
    val vcf = FakeFamily.importVCFGeneric(hc, "data/chr22_1000.vcf")
    println(vcf.count())
    val parents = vcf.filterSamplesList(Set("HG00096",	"HG00097"))
    print(parents.count())
   
    val sampleIds:List[String] = parents.sampleIds.toList.asInstanceOf[List[String]]
    val newIDS = "dsdsdsds" ::  sampleIds
    
    val transRdd = parents.rdd.mapPartitions(x => x.map { case (v, (a, g)) =>
        (v, (a,FakeFamily.createOffspringGeneric(v, g))) }, preservesPartitioning = true)  
        
    val offsrings = parents.copy(rdd =transRdd.asOrderedRDD, sampleIds = newIDS.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(newIDS.length))
    
    ExportVCFEx(offsrings, "target/phasedOffspring.vcf")
  }

  
//  @Test
//  def testRunImportanceAnalysis() {
//    val hc = HailContext(sc)
//    val vcf = hc.importVCF("data/chr22_1000.vcf")
//    val parents = vcf.filterSamplesList(Set("HG00096",	"HG00097"))
//    print(parents.count())
//   
//    val sampleIds:List[String] = parents.sampleIds.toList.asInstanceOf[List[String]]
//    val newIDS = "dsdsdsds" ::  sampleIds
//    
//    val transRdd = parents.rdd.mapPartitions(x => x.map { case (v, (a, g)) =>
//        (v, (a,FakeFamily.createOffspring(v, g))) }, preservesPartitioning = true)  
//    val offsrings = parents.copy(rdd =transRdd.asOrderedRDD, sampleIds = newIDS.toIndexedSeq, 
//        sampleAnnotations =  Annotation.emptyIndexedSeq(newIDS.length))
//    
//    offsrings.exportVCF("target/output.vcf")
///*
// * 
// * 
//    //val v: Variant = null;
//    //val gb = new GenotypeBuilder(v.nAlleles, false)
//    //gb.clear()
//    val pair = GTPair(1,0)
//    println(pair)
//    val gt = Genotype.gtIndex(pair)
//    
//    val gtt = Genotype(gt)
//    val pair1 = GTPair(0,1)
//    println(pair1)
//    val gt1 = Genotype.gtIndex(pair1)
//    //val gsb = new GenotypeStreamBuilder(v.nAlleles, isLinearScale = false)
//    //gsb.write(gb)
//    //val result = gsb.result()
//  
//     */
//  }
  
}