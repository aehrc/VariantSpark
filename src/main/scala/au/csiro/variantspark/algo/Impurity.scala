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

class ClassificationSplitAggregator(val left:ClassificationImpurityAggregator, val right:ClassificationImpurityAggregator) {  
  def reset() {
    left.reset();
    right.reset();
  }
  def getValue(outLeftRight:Array[Double]):Double = {
      outLeftRight(0) = left.getValue()
      outLeftRight(1) = right.getValue()
      return (outLeftRight(0)*left.getCount + outLeftRight(1)*right.getCount)/(left.getCount + right.getCount)   
  }
  
	def initLabel(label:Int) {
	  right.addLabel(label);
	}
	def updateLabel(label:Int) {
	  left.addLabel(label)
	  right.subLabel(label)
	}
}

object ClassificationSplitAggregator {
  def apply(impurity:ClassficationImpurity, nLevels:Int):ClassificationSplitAggregator = new ClassificationSplitAggregator(impurity.createAggregator(nLevels), impurity.createAggregator(nLevels))
}

/**
 * So this will be the main way to interact for algorithms for differnet variable types
 * @author szu004
 */
trait IndexedImpurityCalculator {
	def init(indexes:Array[Int])
	def update(index:Int)
	def getImpurity(outLeftRight:Array[Double]):Double 
}

class ClassificationImpurityCalculator(val labels:Array[Int], val nCategories:Int, val impurity:ClassficationImpurity) extends IndexedImpurityCalculator {
  
  val splitAggregator = ClassificationSplitAggregator(impurity, nCategories);
  
  override def init(indexes:Array[Int]) {
    splitAggregator.reset()
    indexes.foreach(i => splitAggregator.initLabel(labels(i)))
  }
  
	override def update(index:Int) {
	  splitAggregator.updateLabel(labels(index))
	}
	
	override def getImpurity(outLeftRight:Array[Double]):Double = splitAggregator.getValue(outLeftRight)
}
