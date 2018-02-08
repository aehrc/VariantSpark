package au.csiro.variantspark.pedigree.impl


import au.csiro.variantspark.pedigree._
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.utils.defRng
import org.apache.commons.math3.random.RandomGenerator
import java.util.zip.GZIPInputStream
import scala.io.Source
import java.io.FileInputStream
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

/**
 * Create crossing over points based on the provided recombination frequency 
 * distrbution. Which can be loaded from a bed file.
 */

// lets split the distribution by contig
// in general the distribution should just be the probability of splitting at each of the bins 
// the specification of each bin

/**
 * 
 */
case class ContigRecombinationMap(val bins:Array[Long], val recombFreq:Array[Double]) {
  assert(bins.length == recombFreq.length + 1)
  val p = recombFreq.map(rf => rf*1e-8).toArray
  
  /**
   * Draw splits from this distribution
   */
  def drawSplits(rng:XorShift1024StarRandomGenerator):List[Long] = {
    val temp = p.toStream.zip(Stream.continually(rng.nextDouble())).zipWithIndex
    temp.filter({ case ((p,r),i) => r <= p*(bins(i+1) -bins(i))}).map(_._2.toLong).toList
  }
  
  /**
   * Draw a random positon from the bin
   */
  private def splitFromBin(binIndex: Int, rng:XorShift1024StarRandomGenerator):Long =  {
    bins(binIndex) + (bins(binIndex + 1) - bins(binIndex))/2
  }
}

case class RecombinationMap(val contigMap: Map[ContigID, ContigRecombinationMap])  {
  
    def crossingOver(rng: XorShift1024StarRandomGenerator):Map[ContigID, MeiosisSpec] = {
      contigMap.mapValues(cm => MeiosisSpec(cm.drawSplits(rng)))
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
    val mutableContigMap = new HashMap[String, (Buffer[Long], Buffer[Double])]()
    source.getLines().foreach { line => line.split("\t") match {
        case Array(contig, start, end, rr, pos) => 
          val (bins, rates) = mutableContigMap.getOrElseUpdate(contig, (new ArrayBuffer[Long](), new ArrayBuffer[Double]()))
          if (bins.isEmpty) {
            bins+=start.toLong
          }
          bins+=end.toLong
          rates+=rr.toDouble
        case _ => throw new RuntimeException("Illegal line in bed file")
      }
    }
    RecombinationMap(mutableContigMap.toMap
      .mapValues(binsAndRates => ContigRecombinationMap(binsAndRates._1.toArray, binsAndRates._2.toArray)))     
  }
  
  def fromBedFile(pathToBedFile: String): RecombinationMap = {
    LoanUtils.withSource(Source.fromInputStream(new GZIPInputStream(new FileInputStream(pathToBedFile))))(fromSource)  
  }
}

case class HapMapGameteSpecFactory(map: RecombinationMap, seed: Long = defRng.nextLong) extends GameteSpecFactory {
  def createHomozigoteSpec(): GameteSpec = {
    val rng = new XorShift1024StarRandomGenerator(seed)
    GameteSpec(map.crossingOver(rng))
  }
}