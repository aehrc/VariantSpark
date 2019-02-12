package au.csiro.variantspark.algo


trait IndexedSplitter {
  def findSplit(impurityCalc:IndexedImpurityCalculator, splitIndices:Array[Int]):SplitInfo
}