package au.csiro.variantspark.utils





object CV {
  
  type Model = Projector => Double
  // get mean 
  def evaluate(folds:List[Projector])(m:Model):Double = {
    val foldResults:List[Double] = folds.map(f => m(f))
    foldResults.reduce(_+_) / foldResults.size 
  }
  
}