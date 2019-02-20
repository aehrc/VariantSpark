package au.csiro.variantspark.algo


trait IndexedSplitter {
  def findSplit(splitIndices:Array[Int]):SplitInfo
}


trait SubsetSplitterFactory {
  def createSplitter(subsetIndices:Array[Int]):IndexedSplitter
}

class DefSubsetSplitterFactory(val fastThreshold:Int) extends SubsetSplitterFactory {
 
  lazy val defaultSplitter:IndexedSplitter = ???
  lazy val fastSplitter:Option[IndexedSplitter] = ???
  
  def createSplitter(subsetIndices:Array[Int]):IndexedSplitter = {
    // depending on weather the fast memory consuming splitter can be created
    // and the size of the current subset select either the fast memory consuming option
    // slower but memory efficien one
    if (subsetIndices.length >= fastThreshold && !fastSplitter.isEmpty) {
      fastSplitter.get
    } else {
      defaultSplitter
    }
  }
}

