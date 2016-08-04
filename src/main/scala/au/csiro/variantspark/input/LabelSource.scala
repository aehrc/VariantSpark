package au.csiro.variantspark.input

trait LabelSource {
   def getLabels(labels:Seq[String]):Array[Int]
}