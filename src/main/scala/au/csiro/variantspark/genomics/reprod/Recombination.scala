package au.csiro.variantspark.genomics.reprod

import au.csiro.variantspark.genomics._
import java.util.zip.GZIPInputStream
import scala.io.Source
import java.io.FileInputStream
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import au.csiro.variantspark.genomics.ContigSet
import org.apache.commons.math3.random.RandomGenerator
import scala.collection.mutable.ArrayBuilder


/**
 * Recombination map for a single contig
 */
case class ContigRecombinationMap(val bins:Array[Long], val recombFreq:Array[Double]) {  
  def length  = bins.last
  
  lazy val binLengths: Array[Long] =  {
    // mutable version for better performance
    val builder = ArrayBuilder.make[Long]()
    builder.sizeHint(bins.length-1)
    for(i <- 0 until bins.length -1) {
      builder += bins(i+1) - bins(i)
    }
    builder.result()
  }
}

/**
 * Recombination map for a set of contigs
 */
case class RecombinationMap(val contigMap: Map[ContigID, ContigRecombinationMap])  {
  
  def toContigSpecSet:ContigSet = {
    val contigSpecs = contigMap.map({ case (kid, krm) => new ContigSpec(kid, krm.length)});
    ContigSet.apply(contigSpecs.toSeq);
  }

  def filter(contigSet: ContigSet):RecombinationMap = {
     RecombinationMap(contigMap.filterKeys(contigSet.contigIds.contains(_)))
   }
}

object RecombinationMap {
  /**
   * Load the recombination map from a single gziped bed file
   * like the one from fake family, that is formatted as:
   * ContigID Start  End (exclusive) Recombination rate [cM/Mbp] Distance [cm]
   * chr1    0       55549   0.000000        0.000000
   */
  
  def fromSource(source:Source): RecombinationMap = {

    // TODO: Maybe use map builder here or when making a copy
    val mutableContigMap = new HashMap[String, (Buffer[Long], Buffer[Double])]()
    source.getLines().foreach { line => line.split("\t") match {
        //TODO: change to use regex
        case Array(chr, start, end, rr, pos) => 
          val contig = chr.substring(3)
          val (bins, rates) = mutableContigMap.getOrElseUpdate(contig, (new ArrayBuffer[Long](), new ArrayBuffer[Double]()))
          if (bins.isEmpty) {
            bins+=start.toLong
          }
          bins+=end.toLong
          rates+=rr.toDouble
        case _ => throw new RuntimeException("Illegal line in bed file")
      }
    }
    val mapLike = mutableContigMap
      .mapValues(binsAndRates => ContigRecombinationMap(binsAndRates._1.toArray, binsAndRates._2.toArray))
    RecombinationMap(mapLike.toMap)
  }
  
  def fromBedFile(pathToBedFile: String): RecombinationMap = {
    LoanUtils.withSource(Source.fromInputStream(new GZIPInputStream(new FileInputStream(pathToBedFile))))(fromSource)  
  }
}