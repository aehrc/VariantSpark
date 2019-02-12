package au.csiro.variantspark.algo

trait ImpurityAggregator {
    def reset()
	  def getValue(outLeftRight:Array[Double]):Double
}

trait ClassificationImpurityAggregator extends  ImpurityAggregator {
	def initLabel(label:Int)
	def updateLabel(label:Int)
}

trait RegressionImpurityAggregator extends  ImpurityAggregator {
	def initValue(value:Double)
	def updateValue(value:Double)
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

class ClassificationImpurityCalculator(val labels:Array[Int], val nCategories:Int, impurity:ClassificationImpurityAggregator) extends IndexedImpurityCalculator {
  
  override def init(indexes:Array[Int]) {
    impurity.reset()
    indexes.foreach(i => impurity.initLabel(labels(i)))
  }
  
	override def update(index:Int) {
	  impurity.updateLabel(labels(index))
	}
	
	override def getImpurity(outLeftRight:Array[Double]):Double = impurity.getValue(outLeftRight)
}
