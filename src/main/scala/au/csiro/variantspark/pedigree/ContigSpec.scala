package au.csiro.variantspark.pedigree

import java.io.InputStream
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import scala.io.Source

/**
##contig=<ID=1,length=249250621,assembly=b37>
##contig=<ID=2,length=243199373,assembly=b37>
##contig=<ID=3,length=198022430,assembly=b37>
##contig=<ID=4,length=191154276,assembly=b37>
##contig=<ID=5,length=180915260,assembly=b37>
##contig=<ID=6,length=171115067,assembly=b37>
##contig=<ID=7,length=159138663,assembly=b37>
##contig=<ID=8,length=146364022,assembly=b37>
##contig=<ID=9,length=141213431,assembly=b37>
##contig=<ID=10,length=135534747,assembly=b37>
##contig=<ID=11,length=135006516,assembly=b37>
##contig=<ID=12,length=133851895,assembly=b37>
##contig=<ID=13,length=115169878,assembly=b37>
##contig=<ID=14,length=107349540,assembly=b37>
##contig=<ID=15,length=102531392,assembly=b37>
##contig=<ID=16,length=90354753,assembly=b37>
##contig=<ID=17,length=81195210,assembly=b37>
##contig=<ID=18,length=78077248,assembly=b37>
##contig=<ID=19,length=59128983,assembly=b37>
##contig=<ID=20,length=63025520,assembly=b37>
##contig=<ID=21,length=48129895,assembly=b37>
##contig=<ID=22,length=51304566,assembly=b37>
##contig=<ID=X,length=155270560,assembly=b37>
##contig=<ID=Y,length=59373566,assembly=b37>
**/

case class ContigSpec(val id:String, val length:Long)  {
  
  def isAutosome:Boolean = id match { case ContigSpec.autosome() => true case _  => false }
  def isSex:Boolean = ("X" == id || "Y" == id)
  def isChromosome = isAutosome || isSex
}

object ContigSpec {
 
  private val autosome = raw"\d+".r
  private val contigHeader = raw"##contig=<ID=(\w+),length=(\d+),assembly=(\w+)>".r
  
  def parseVcfHeaderLine(headerLine:String):ContigSpec =  {
    headerLine match {
      case contigHeader(contig, length, assembly) => ContigSpec(contig, length.toLong)
    }
  }
}

case class ContigSet(val contigs: Seq[ContigSpec]) {
  def totalLenght:Long = contigs.map(_.length).sum
  def filter(predicate: ContigSpec=> Boolean):ContigSet = ContigSet(contigs.filter(predicate))
  def onlyAutosomes() = filter(_.isAutosome)
  def onlyChromosomes() = filter(_.isChromosome)
  def contigIds:Set[ContigID] = contigs.map(_.id).toSet
}

object ContigSet {

  implicit case object ContigSpecOrdering extends Ordering[ContigSpec] {
    def compare(x: ContigSpec, y: ContigSpec): Int = {
      if (x.isAutosome && y.isAutosome) x.id.toInt.compareTo(y.id.toInt) else x.id.compareTo(y.id)
    }
  }

  def fromUnsorted(contigs: Seq[ContigSpec]) = apply(contigs.sorted)
  
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

object ReferenceContigSet {
  val b37  = ContigSet.fromResource("ref-contigs/b37.vcf")
}



