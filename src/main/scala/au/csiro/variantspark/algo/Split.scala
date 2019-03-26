package au.csiro.variantspark.algo


trait IndexedSplitAggregator {
  def left: ImpurityAggregator
  def right: ImpurityAggregator  
  def reset() {
    left.reset();
    right.reset();    
  }
	def update(agg: ImpurityAggregator) {
	  left.add(agg)
	  right.sub(agg)	  
	}
	def getValue(outSplitImp:SplitImpurity):Double = {
	  left.splitValue(right, outSplitImp)
	}
	def init(indexes:Array[Int]) {
    reset()
    indexes.foreach(i => init(i))	  
	}
	def init(index:Int)
	def update(index:Int)
}

class ClassificationSplitAggregator private (val labels:Array[Int], val left:ClassificationImpurityAggregator, val right:ClassificationImpurityAggregator) extends IndexedSplitAggregator {  
  
  def initLabel(label:Int) {
	  right.addLabel(label);
	}
	
	def updateLabel(label:Int) {
	  left.addLabel(label)
	  right.subLabel(label)
	}

	@Override
	def init(index:Int) = initLabel(labels(index))

	@Override
	def update(index:Int) = updateLabel(labels(index)) 
}

object ClassificationSplitAggregator {
  def apply(impurity:ClassficationImpurity, labels:Array[Int], nCategories:Int):ClassificationSplitAggregator = new ClassificationSplitAggregator(labels, impurity.createAggregator(nCategories), impurity.createAggregator(nCategories))
}

class ConfusionAggregator(val matrix:Array[ClassificationImpurityAggregator], val labels:Array[Int]) {
  
  def this(impurity:ClassficationImpurity, size:Int, nCategories:Int, labels:Array[Int]) {
    this(Array.fill(size)(impurity.createAggregator(nCategories)), labels)
  }
  
  /**
   * Reset the first nLevels of the matrix
   */
  def reset(nLevels:Int) {
    assert(nLevels <= matrix.length)
    matrix.iterator.take(nLevels).foreach(_.reset());
  }
  
  /**
   * Add a response at index yIndex for ordinal level
   */
  def updateAt(level:Int, yIndex:Int) = matrix(level).addLabel(labels(yIndex)); 
  
  def apply(level:Int):ClassificationImpurityAggregator = matrix(level) 
}


trait IndexedSplitter {
  def findSplit(splitIndices:Array[Int]):SplitInfo
}


trait SplitterFactory {
  def createSplitter(impCalc:IndexedSplitAggregator): IndexedSplitter  
}

trait FastSplitterFactory extends SplitterFactory {
  def confusionSize:Int
  def createSplitter(impCalc:IndexedSplitAggregator, confusionAgg:ConfusionAggregator): IndexedSplitter    
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

