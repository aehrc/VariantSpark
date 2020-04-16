package au.csiro.variantspark.work

import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.pbdava.ssparkle.common.utils.Timed

object RandomnesTest {

  def main(argv: Array[String]) {
    println("Hello")

    val seed = 13
    val rng = new XorShift1024StarRandomGenerator(seed)

    val splitNo = 100
    val splits = Array.fill(splitNo)(0)
    val varNo = 1000000
    val mtry = 10000
    val mtryFraction = mtry.toDouble / varNo.toDouble

    Range(0, varNo).foreach { varId =>
      Range(0, splitNo).foreach { splitIndex =>
        if (rng.nextDouble() <= mtryFraction) {
          splits(splitIndex) = splits(splitIndex) + 1
        }
      }

    }

    println(splits.toList)

  }
}
