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
import scala.collection.mutable.ListBuffer

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
    
    // mutable version for better performance
    val result = ListBuffer[Long]()
    for (i <- 0 until p.length) {
      if ( p(i)*(bins(i+1)-bins(i)) >= rng.nextDouble()) {
        result+= splitFromBin(i, rng)
      }
    }
    result.toList
    //val temp = p.toStream.zip(Stream.continually(rng.nextDouble())).zipWithIndex
    //temp.filter({ case ((p,r),i) => r <= p*(bins(i+1) -bins(i))}).map(_._2.toLong).toList
  }
  
  /**
   * Draw a random positon from the bin
   */
  private def splitFromBin(binIndex: Int, rng:XorShift1024StarRandomGenerator):Long =  {
    bins(binIndex) + (bins(binIndex + 1) - bins(binIndex))/2
  }
}

case class RecombinationMap(val contigMap: Map[ContigID, ContigRecombinationMap])  {
   assert(contigMap.isInstanceOf[Serializable])
 
  def crossingOver(rng: XorShift1024StarRandomGenerator):Map[ContigID, MeiosisSpec] = {
    contigMap.mapValues(cm => MeiosisSpec(cm.drawSplits(rng), rng.nextInt(2)))
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
    // make a copy to  create a serializable Map
    RecombinationMap(Map(mapLike.toArray: _*))
  }
  
  def fromBedFile(pathToBedFile: String): RecombinationMap = {
    LoanUtils.withSource(Source.fromInputStream(new GZIPInputStream(new FileInputStream(pathToBedFile))))(fromSource)  
  }
}

case class HapMapMeiosisSpecFactory(map: RecombinationMap, seed: Long = defRng.nextLong) extends MeiosisSpecFactory {
  
  val rng = new XorShift1024StarRandomGenerator(seed)  
  def createMeiosisSpec(): Map[ContigID, MeiosisSpec] = map.crossingOver(rng).toSeq.toMap
}

object HapMapMeiosisSpecFactory {
  def fromBedFile(pathToBedFile: String, seed: Long = defRng.nextLong): HapMapMeiosisSpecFactory = {
    HapMapMeiosisSpecFactory(RecombinationMap.fromBedFile(pathToBedFile), seed)
  }
}
