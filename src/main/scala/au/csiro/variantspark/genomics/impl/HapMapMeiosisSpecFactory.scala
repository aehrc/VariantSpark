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


case class HapMapMeiosisSpecFactory(map: RecombinationMap, seed: Long = defRng.nextLong) extends MeiosisSpecFactory {
  val rng = new XorShift1024StarRandomGenerator(seed)  
  def createMeiosisSpec(): Map[ContigID, MeiosisSpec] = map.crossingOver(rng).toSeq.toMap
}

