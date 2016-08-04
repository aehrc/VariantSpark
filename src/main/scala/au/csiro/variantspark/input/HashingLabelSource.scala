package au.csiro.variantspark.input



class HashingLabelSource(val maxLabel:Int) extends LabelSource {
  def getLabels(labels:Seq[String]):Array[Int] = labels.map(l => (math.abs(l.hashCode()) % maxLabel).toInt).toArray
}