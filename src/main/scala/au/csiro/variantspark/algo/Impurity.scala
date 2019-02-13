package au.csiro.variantspark.algo

import au.csiro.variantspark.algo.impurity.GiniImpurityAggregator

trait ImpurityAggregator {
  def reset()
  def isEmpty():Boolean
  def add(other:ImpurityAggregator)
  def sub(other:ImpurityAggregator)
  def getValue():Double
  def getCount():Int
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


class SplitImpurity(var left:Double, var right:Double) {
  def this() {
    this(0.0,0.0)
  }
}

class ClassificationSplitAggregator(val left:ClassificationImpurityAggregator, val right:ClassificationImpurityAggregator) {  
  def reset() {
    left.reset();
    right.reset();
  }
  def getValue(outSplitImp:SplitImpurity):Double = {
      outSplitImp.left = left.getValue()
      outSplitImp.right = right.getValue()
      return (outSplitImp.left*left.getCount + outSplitImp.right*right.getCount)/(left.getCount + right.getCount)   
  }
  
	def initLabel(label:Int) {
	  right.addLabel(label);
	}
	def updateLabel(label:Int) {
	  left.addLabel(label)
	  right.subLabel(label)
	}

	def update(agg: ImpurityAggregator) {
	  left.add(agg)
	  right.sub(agg)
	}


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


/**
 * So this will be the main way to interact for algorithms for ml tasks (classification and regression)
 * @author szu004
 */
trait IndexedImpurityCalculator {
	def init(indexes:Array[Int])
	def update(index:Int)
	def update(agg: ImpurityAggregator)
	def getConfusionAggregator(nLevels:Int):ConfusionAggregator
	def getImpurity(outSplitImp:SplitImpurity):Double 
}

class ClassificationImpurityCalculator(val labels:Array[Int], val nCategories:Int, val impurity:ClassficationImpurity) extends IndexedImpurityCalculator {
  
  val splitAggregator = ClassificationSplitAggregator(impurity, nCategories);
  lazy val confusionAggregator = new ConfusionAggregator(impurity, 10, nCategories, labels)
  
  override def init(indexes:Array[Int]) {
    splitAggregator.reset()
    indexes.foreach(i => splitAggregator.initLabel(labels(i)))
  }
  
	override def update(yIndex:Int) {
	  splitAggregator.updateLabel(labels(yIndex))
	}
	
	override def getImpurity(outSplitImp:SplitImpurity):Double = splitAggregator.getValue(outSplitImp)

  override def update(agg: ImpurityAggregator) {
    splitAggregator.update(agg)
  }
	
	override def getConfusionAggregator(nLevels:Int):ConfusionAggregator  = {
	  confusionAggregator.reset(nLevels)
	  confusionAggregator
	}
}
