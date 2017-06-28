package au.csiro.variantspark.utils

object CV {
  
  type Model = Projector => Double

  def evaluateMean(folds:List[Projector])(m:Model):Double = {
    val foldResults:List[Double] = folds.map(f => m(f))
    foldResults.reduce(_+_) / foldResults.size 
  }
  
}