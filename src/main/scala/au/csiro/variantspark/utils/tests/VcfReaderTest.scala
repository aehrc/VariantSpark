package au.csiro.variantspark.utils.tests

import htsjdk.variant.vcf.VCFFileReader
import java.io.File
import collection.JavaConverters._

object VcfReaderTest {
  
  
  def main(args:Array[String]) {
    println("Hello")
    
    val reader = new VCFFileReader(new File("data/small.vcf"), false)
    println("Header" + reader.getFileHeader())
    
    val vi = reader.iterator().asScala.next()
    println(vi)
    val gts = vi.getGenotypes()
    println("Lazy: " + gts.isLazyWithData())
    println(vi.getAlleles())
    println(vi.getAlternateAlleles())
    vi.getGenotypesOrderedByName().iterator().asScala.foreach{gt =>
    println(gt)    
    println(gt.getPloidy())
    println(gt.isHet())    
    println(gt.isHomRef())    
    println(gt.isHomVar())
    println(gt.isHetNonRef())
    }
  }
}