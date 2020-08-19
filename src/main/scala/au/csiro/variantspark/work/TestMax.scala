package au.csiro.variantspark.work

object TestMax {

  def main(argv: Array[String]): Unit = {
    println("TestMax")

    val testCounts = Array(0, 3, 1, 2, 3)

    println(testCounts.zipWithIndex.max._2)
    println(testCounts.zipWithIndex.maxBy(_._1)._2)

  }
}
