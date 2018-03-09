package au.csiro.variantspark.genomics

import java.io.InputStream
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import scala.io.Source

/**
 * A contig specification
 */
case class ContigSpec(val id:String, val length:Long)  {
  
  def isAutosome:Boolean = id match { case ContigSpec.autosome() => true case _  => false }
  def isSex:Boolean = ("X" == id || "Y" == id)
  def isChromosome = isAutosome || isSex
}

object ContigSpec {
 
  private val autosome = raw"\d+".r
  private val contigHeader = raw"##contig=<ID=(\w+),length=(\d+).*>".r
  
  def parseVcfHeaderLine(headerLine:String):ContigSpec =  {
    headerLine match {
      case contigHeader(contig, length) => ContigSpec(contig, length.toLong)
    }
  }
}

/**
 * A set of contig specifications
 */
case class ContigSet(val contigs: Set[ContigSpec]) {
  
  
  def totalLenght:Long = contigs.map(_.length).sum
  def filter(predicate: ContigSpec=> Boolean):ContigSet = ContigSet(contigs.filter(predicate))
  def onlyAutosomes() = filter(_.isAutosome)
  def onlyChromosomes() = filter(_.isChromosome)
  def contigIds:Set[ContigID] = contigs.map(_.id).toSet
  def toSeq:Seq[ContigSpec] = contigs.toSeq
  def toCannonicalSeq:Seq[ContigSpec] = {
    toSeq.sorted(ContigSet.ContigSpecOrdering)
  }
}

object ContigSet {

  implicit case object ContigSpecOrdering extends Ordering[ContigSpec] {
    def compare(x: ContigSpec, y: ContigSpec): Int = {
      if (x.isAutosome && y.isAutosome) x.id.toInt.compareTo(y.id.toInt) else x.id.compareTo(y.id)
    }
  }

  def apply(contigs: Seq[ContigSpec]):ContigSet = apply(contigs.toSet)
  
  def fromResource(resourcePath:String): ContigSet = {
    fromVcfHeader(getClass.getClassLoader.getResourceAsStream(resourcePath))
  }
  
  def fromVcfHeader(input: =>InputStream): ContigSet = {   
    val contigs = LoanUtils.withCloseable(input) { is =>
      Source.fromInputStream(is).getLines()
        .filter(_.startsWith("##contig="))
        .map(ContigSpec.parseVcfHeaderLine).toList
    }
    ContigSet(contigs)
  }
}

/**
 * Contig sets for genome reference builds
 */
object ReferenceContigSet {
  val b37  = ContigSet.fromResource("ref-contigs/b37.vcf")
}
