package au.csiro.variantspark.algo

import au.csiro.variantspark.algo.impurity.GiniImpurityAggregator

class SplitImpurity(var left:Double, var right:Double) {
  def this() {
    this(0.0,0.0)
  }
  def set(left:Double, right:Double) {
    this.left = left
    this.right = right
  }
}

trait ImpurityAggregator {
  def reset()
  def isEmpty():Boolean
  def add(other:ImpurityAggregator)
  def sub(other:ImpurityAggregator)
  def getValue():Double
  def getCount():Int
  def splitValue(other:ImpurityAggregator, out:SplitImpurity):Double 
}

trait ClassificationImpurityAggregator extends  ImpurityAggregator {
	def addLabel(label:Int)
	def subLabel(label:Int)
}

trait RegressionImpurityAggregator extends  ImpurityAggregator {
	def addValue(value:Double)
	def subValue(value:Double)
}


trait Impurity

trait ClassficationImpurity extends Impurity {
  def createAggregator(nLevels:Int): ClassificationImpurityAggregator
}

case object GiniImpurity extends ClassficationImpurity {
  def createAggregator(nLevels:Int): ClassificationImpurityAggregator = new GiniImpurityAggregator(nLevels)  
}

trait IndexedSplitAggregator {
  def reset()
	def init(index:Int)
	def update(index:Int)
	def update(agg: ImpurityAggregator)
	def getValue(outSplitImp:SplitImpurity):Double 
	def init(indexes:Array[Int]) {
    reset()
    indexes.foreach(i => init(i))	  
	}
}

class ClassificationSplitAggregator(val left:ClassificationImpurityAggregator, val right:ClassificationImpurityAggregator) extends IndexedSplitAggregator {  
  
  def initLabel(label:Int) {
	  right.addLabel(label);
	}
	
	def updateLabel(label:Int) {
	  left.addLabel(label)
	  right.subLabel(label)
	}

  @Override
  def reset() {
    left.reset();
    right.reset();
  }
  
  @Override
  def getValue(outSplitImp:SplitImpurity):Double = {
    return left.splitValue(right, outSplitImp)
  }
  
	@Override
	def update(agg: ImpurityAggregator) {
	  left.add(agg)
	  right.sub(agg)
	}

	@Override
	def init(index:Int) = initLabel(index)

	@Override
	def update(index:Int) = updateLabel(index) 
}

object ClassificationSplitAggregator {
  def apply(impurity:ClassficationImpurity, nLevels:Int):ClassificationSplitAggregator = new ClassificationSplitAggregator(impurity.createAggregator(nLevels), impurity.createAggregator(nLevels))
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