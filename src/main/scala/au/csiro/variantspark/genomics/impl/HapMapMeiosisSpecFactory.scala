package au.csiro.variantspark.genomics.impl


import au.csiro.variantspark.genomics._
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
import au.csiro.variantspark.genomics.reprod.RecombinationMap
import au.csiro.variantspark.genomics.reprod.MeiosisSpec
import au.csiro.variantspark.genomics.reprod.MeiosisSpecFactory
import au.csiro.variantspark.genomics.reprod.ContigRecombinationMap

case class ContigRecombinationDistribution(val bins:Array[Long], val p:Array[Double]) {
  assert(bins.length == p.length + 1)  
  
  def crossingOver(rng: RandomGenerator):MeiosisSpec = {
    MeiosisSpec(drawSplits(rng), rng.nextInt(2))
  }
  
  /**
   * Draw splits from this distribution
   */
  def drawSplits(rng:RandomGenerator):List[Long] = {
    
    // mutable version for better performance
    val result = ListBuffer[Long]()
    for (i <- 0 until p.length) {
      if ( p(i) >= rng.nextDouble()) {
        result+= splitFromBin(i, rng)
      }
    }
    result.toList
  }
  def length  = bins.last
  /**
   * Draw a random positon from the bin
   */
  private def splitFromBin(binIndex: Int, rng:RandomGenerator):Long =  {
    bins(binIndex) + (bins(binIndex + 1) - bins(binIndex))/2
  }
  
  
}

object ContigRecombinationDistribution {

  /**
   * Recombination rare is in cM per Mbp so the conversion factors is 1e-2 * 1e-6
   */
  val conversionFactor = 1e-8
  def fromRecombiationMap(crm:ContigRecombinationMap): ContigRecombinationDistribution = {
    val p:Array[Double] =  crm.recombFreq.map(rf => rf*conversionFactor)
                .zip(crm.binLengths).map(t => t._1 * t._2)  
    ContigRecombinationDistribution(crm.bins, p)
  }
}

case class RecombinationDistribution(val contigMap: Map[ContigID, ContigRecombinationDistribution])  {
   assert(contigMap.isInstanceOf[Serializable])
 
  def crossingOver(rng: RandomGenerator):Map[ContigID, MeiosisSpec] = {
    contigMap.mapValues(cm => cm.crossingOver(rng))
  }
}

object RecombinationDistribution {
  def fromRecombiationMap(rm: RecombinationMap): RecombinationDistribution = 
     RecombinationDistribution(Map(rm.contigMap.mapValues(ContigRecombinationDistribution.fromRecombiationMap).toArray: _*))
}

case class HapMapMeiosisSpecFactory(map: RecombinationDistribution)(implicit rng: RandomGenerator) extends MeiosisSpecFactory {
  def createMeiosisSpec(): Map[ContigID, MeiosisSpec] = map.crossingOver(rng).toSeq.toMap
}

object HapMapMeiosisSpecFactory {
  def apply(map: RecombinationMap, seed: Long = defRng.nextLong):HapMapMeiosisSpecFactory = {
    implicit val rng = new XorShift1024StarRandomGenerator(seed) 
    HapMapMeiosisSpecFactory(map)
  }

  def apply(map: RecombinationMap)(implicit rng: RandomGenerator):HapMapMeiosisSpecFactory = {
    HapMapMeiosisSpecFactory(RecombinationDistribution.fromRecombiationMap(map))
  }

}


