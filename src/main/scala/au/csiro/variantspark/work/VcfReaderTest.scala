package au.csiro.variantspark.work

import htsjdk.variant.vcf.VCFFileReader
import java.io.File
import collection.JavaConverters._
import htsjdk.variant.variantcontext.GenotypesContext

object VcfReaderTest {

  def main(args: Array[String]) {
    println("Hello")

    val reader = new VCFFileReader(new File("data/chr22_1000.vcf"), false)
    val header = reader.getFileHeader()

    println("Header" + header)

    println(header.getGenotypeSamples())

    val vi = reader.iterator().asScala.next()
    println(vi)

    println("Contig: " + vi.getContig())
    println("End: " + vi.getEnd())
    val gts: GenotypesContext = vi.getGenotypes()

    println("Lazy: " + gts.isLazyWithData())
    println("Mutable: " + gts.isMutable())
    println(vi.getAlleles())
    println(vi.getAlternateAlleles())
    vi.getGenotypesOrderedByName().iterator().asScala.take(1).foreach { gt =>
      println(gt)
      println(gt.getPloidy())
      println(gt.isHet())
      println(gt.isHomRef())
      println(gt.isHomVar())
      println(gt.isHetNonRef())
    }
  }
}
